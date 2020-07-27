package org.chamhaw.tools.leaderelection.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
@Slf4j
public class ZkUtils {

    public static void updatePersistentPath(CuratorFramework curator, String path, byte[] ba) throws Exception {
        try {
            curator.setData().forPath(path, ba);
        } catch (KeeperException.NoNodeException e) {
            createPersistentPath(curator, path, ba);
        } catch (KeeperException.NodeExistsException e) {
            logger.info("update persistentPath exception.", e);
        }
    }

    public static void createPersistentPath(CuratorFramework curator, String path, byte[] ba) throws Exception {
        try {
            if (path == null) {
                return;
            }
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path, ba);
        } catch (KeeperException.NodeExistsException ex) {
            logger.debug("no node for path " + path);
        }
    }

    public static String readData(CuratorFramework curator, String path) throws Exception {
        String data = null;
        try {
            data = new String(curator.getData().forPath(path));
        } catch (KeeperException.NoNodeException e) {
            logger.debug("no node for path " + path);
        }
        return data;
    }

    public static boolean deletePath(CuratorFramework curator, String path) throws Exception {
        try {
            curator.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            logger.debug("deletePath exception.", e);
        }
        return true;
    }

    public static void createEphemeralPath(CuratorFramework curator, String path, byte[] ba) throws Exception {
        try {
            if (path == null) {
                return;
            }
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path, ba);
        } catch (KeeperException.NodeExistsException ex) {
            logger.info("create node is exist for path " + path);
            updateEphemeralPath(curator, path, ba);
        }
    }

    public static void updateEphemeralPath(CuratorFramework curator, String path, byte[] ba) throws Exception {
        try {
            curator.setData().forPath(path, ba);
        } catch (KeeperException.NoNodeException e) {
            logger.info("No node : " + path );
            createEphemeralPath(curator, path, ba);
        }
    }

    public static boolean checkExists(CuratorFramework curator, String path) throws Exception {
        return null != curator.checkExists().forPath(path);
    }

    public static List<String> getChild(CuratorFramework curator, String path) throws Exception {
        try {
            List<String> result = curator.getChildren().forPath(path);
            return result;
        } catch (KeeperException.NoNodeException e) {
            logger.warn("no zookeeper config for the path" + path);
            return Collections.emptyList();
        }
    }

    public static void clearExpiredEphemeralNode(CuratorFramework curator, String path) {
        ZooKeeper zookeeper;
        try {
            zookeeper = curator.getZookeeperClient().getZooKeeper();
        } catch (Exception e) {
            logger.error("clearExpiredEphemeralNode getZooKeeper exception.", e);
            throw new IllegalArgumentException("clearExpiredEphemeralNode getZooKeeper exception.");
        }
        if (zookeeper == null) {
            return;
        }
        long nowSessionId = zookeeper.getSessionId();
        Stat stat;
        try {
            stat = zookeeper.exists(path, false);
            if ((stat != null) && (stat.getEphemeralOwner() != nowSessionId)) {
                logger.info("clear Ephemeral node {}",path);
                deletePath(curator, path);
            }
        } catch (Exception e) {
            logger.warn("clearExpiredEphemeralNode zookeeper.exists KeeperException.", e);
        }
    }

    public static void registerService(CuratorFramework curator, String path) {
        try {
            ZkUtils.clearExpiredEphemeralNode(curator, path);
            ZkUtils.createEphemeralPath(curator, path, ServerStatus.READY.name().getBytes());
        } catch (Exception e) {
            logger.error("write server status failed exception:", e);
        }
    }

    public static void listenServerStateChanged(CuratorFramework curator, String path) {
        curator.getConnectionStateListenable().addListener((client, newState) -> {
            if (ConnectionState.LOST == newState) {
                logger.info("ServerListener >> server state changed : connect state lost");
            } else if (ConnectionState.RECONNECTED == newState) {
                logger.info("ServerListener >> server state changed : connect state reconnected, registerService...");
                registerService(curator, path);
            }
        });
    }
}
