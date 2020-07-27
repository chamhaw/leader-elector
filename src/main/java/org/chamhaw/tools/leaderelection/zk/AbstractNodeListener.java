package org.chamhaw.tools.leaderelection.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;


public abstract class AbstractNodeListener implements TreeCacheListener {

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        switch (event.getType()) {
            case NODE_ADDED:
                onNodeAdded(client, event);
                break;
            case NODE_REMOVED:
                onNodeRemoved(client, event);
                break;
            case NODE_UPDATED:
                onNodeUpdated(client, event);
                break;
            default:
                return;
        }
    }

    protected void onNodeUpdated(CuratorFramework client, TreeCacheEvent event) throws Exception {
        //Do nothing
    }

    protected void onNodeRemoved(CuratorFramework client, TreeCacheEvent event) throws Exception {
        //Do nothing
    }

    protected void onNodeAdded(CuratorFramework client, TreeCacheEvent event) throws Exception {
        //Do nothing
    }
}
