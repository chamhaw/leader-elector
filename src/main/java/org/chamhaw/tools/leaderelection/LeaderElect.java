package org.chamhaw.tools.leaderelection;


public interface LeaderElect {

    void init(Callback callback) throws Exception;

    void leaderElect() throws Exception;

    String getLeader() throws Exception;

    void shutdown();
}
