package org.chamhaw.tools.leaderelection;

/**
 * @Author: hao.zhang
 * @Date: 2019-05-22 13:51
 */
public interface Callback {
    void obtainLeaderShip(Object arg);

    void loseLeaderShip(Throwable ex, Object arg);
}
