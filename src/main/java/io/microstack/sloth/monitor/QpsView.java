package io.microstack.sloth.monitor;

/**
 * Created by imotai on 2016/9/29.
 */
public class QpsView {
    private long ctime;
    private double qps;

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }

    public double getQps() {
        return qps;
    }

    public void setQps(double qps) {
        this.qps = qps;
    }
}
