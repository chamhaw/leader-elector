package org.chamhaw.tools.leaderelection.zk;

import org.chamhaw.tools.leaderelection.Callback;
import org.chamhaw.tools.leaderelection.LeaderElect;
import org.chamhaw.tools.leaderelection.util.NamedThreadFactory;
import org.chamhaw.tools.leaderelection.util.NetworkUtil;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Author: hao.zhang
 * Date: 5/10/19 11:32 AM
 */
@Slf4j
public class ZookeeperLeaderElect implements LeaderElect {


    private static final Long moretime = 10000L; //比session超时时间多等待十秒

    private static final String LEADER_ROOT_PATH = "/leader";
    private static final String LEADER_ELECT_PATH = "/leader/latch";
    private static final String LEADER_HOST_PATH = "/leader/host";
    
    private CuratorFramework curator;

    private List<String> priorLeaderHost;

    public ZookeeperLeaderElect(CuratorFramework curator) {
        this(curator, Collections.emptyList());
    }

    public ZookeeperLeaderElect(CuratorFramework curator, List<String> priorLeaderHost) {
        this.curator = curator;
        this.priorLeaderHost = priorLeaderHost;
    }

    private String currentLeader;


    private ScheduledExecutorService executorService;

    private Callback callback;

    private volatile boolean toShutdown = false;

    /**
     * 临时节点session与本机的不一样不意味着当前没有leader，可能是由其他机器保活。
     */

    @Override
    public void init(Callback callback) throws Exception {
        logger.info("Initializing Zookeeper Leader Election...");
        this.callback = callback;
        addLeaderLoseListener();
        checkLeaderTask();
    }

    @Override
    public void leaderElect() throws Exception {
        if (toShutdown) {
            logger.info("This server has been to pause. Give up leader election.");
            return;
        }
        if (anyLeader()) {
            logger.info("await ephemeral node /leader/host lose...");
            Integer sessionTimeout = curator.getZookeeperClient().getConnectionTimeoutMs();
            try {
                Thread.sleep(sessionTimeout + moretime);
            } catch (InterruptedException e) {
                logger.error("await ephemeral node lost exception", e);
                throw e;
            }
            logger.info("priorLeaderHost list:{}", priorLeaderHost);
            if (!priorLeaderHost.contains(NetworkUtil.getLocalIp())) {
                Thread.sleep(sessionTimeout + moretime);
                logger.info("current ip less prior, await more time");
                return;
            }
        }
        if (anyLeader()) {
            logger.info("leader has been existed");
            return;
        }
        runLeaderLatch(curator);
        String newLeader = getLeader();
        if (null != newLeader) {
            currentLeader = newLeader;
        }
        logger.info("currentLeader is " + currentLeader);
    }

    private void checkLeaderTask() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
        executorService = Executors
                .newScheduledThreadPool(1, new NamedThreadFactory("checkLeader Thread"));
        executorService.scheduleWithFixedDelay(() -> {
            try {
                String leader = getLeader();
                if (Strings.isNullOrEmpty(leader)) {
                    Thread.sleep(3000);
                    if (Strings.isNullOrEmpty(getLeader())) {
                        leaderElect();
                    }
                } else {
                    if (!leader.equals(NetworkUtil.getLocalhostWithPort())) {
                        logger.debug("current server isn't leader.");
                        loseLeaderShip();
                    }
                }
            } catch (Exception e) {
                logger.warn("checkLeaderTask exception.", e);
            }
        }, 120, 60, TimeUnit.SECONDS);
    }

    private void addLeaderLoseListener() throws Exception {
        TreeCache leaderNodeTreeCache = new TreeCache(curator, LEADER_ROOT_PATH);
        leaderNodeTreeCache.start();
        leaderNodeTreeCache.getListenable().addListener(new AbstractNodeListener() {
            @Override
            protected void onNodeRemoved(CuratorFramework client, TreeCacheEvent event) throws Exception {
                String path = event.getData().getPath();
                if (path.equals(LEADER_HOST_PATH) && client.getState() == CuratorFrameworkState.STARTED && !anyLeader()) {
                    logger.info("leader node lose");
                    try {
                        leaderElect();
                    } catch (Exception e) {
                        logger.error("reLeaderElect exception ...", e);
                    }
                }
            }

            @Override
            protected void onNodeAdded(CuratorFramework client, TreeCacheEvent event) throws Exception {
                String path = event.getData().getPath();
                if (path.equals(LEADER_HOST_PATH) && client.getState() == CuratorFrameworkState.STARTED && anyLeader()) {
                    logger.info("elected a leader {}", new String(event.getData().getData()));
                }
            }
        });
    }



    private void runLeaderLatch(CuratorFramework curator) {
        logger.info("run LeaderLatch path:{}", ZookeeperLeaderElect.LEADER_ELECT_PATH);
        try (LeaderLatch leaderLatch = new LeaderLatch(curator, ZookeeperLeaderElect.LEADER_ELECT_PATH, NetworkUtil.getLocalhostWithPort(), LeaderLatch.CloseMode.NOTIFY_LEADER)) {
            leaderLatch.start();
            leaderLatch.await();
            logger.info("leaderLatch ended successfully. try to obtainLeaderShip.");
            obtainLeaderShip();
        } catch (Exception e) {
            logger.error("leaderElect failed.", e);
            throw new RuntimeException("leaderElect failed");
        }
    }

    private boolean anyLeader() throws Exception {
        return ZkUtils.checkExists(curator, LEADER_HOST_PATH);
    }

    @Override
    public String getLeader() throws Exception {
        return ZkUtils.readData(curator, LEADER_HOST_PATH);
    }

    private void obtainLeaderShip() {
        if (toShutdown) {
            logger.info("This server has been to pause. Give up leader election.");
            return;
        }
        logger.info("obtain leadership:{}", NetworkUtil.getLocalhostWithPort());
        try {
            if (!anyLeader()) {
                logger.info("elect leader is {}", NetworkUtil.getLocalIp());
                ZkUtils.createEphemeralPath(curator, LEADER_HOST_PATH, NetworkUtil.getLocalhostWithPort().getBytes());
                if (null != currentLeader && currentLeader.equals(NetworkUtil.getLocalhostWithPort())) {
                    //leader没变，则不做变更。
                    return;
                }
                if (callback != null) {
                    callback.obtainLeaderShip(null);
                } else {
                    logger.warn("No onSuccess callback function defined so that no external action happened.");
                }
            }
        } catch (Exception e) {
            logger.error("onSuccess exception ", e);
        }
    }

    private void loseLeaderShip() {
        if (callback != null) {
            callback.loseLeaderShip(null, null);
        } else {
            logger.warn("No onFail callback function defined so that no external action happened.");
        }
    }


    @Override
    public void shutdown() {
        this.toShutdown = true;
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // do nothing
        }
        // close方法是安全的
        curator.close();
    }
}
