package io.microstack.sloth.log;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by imotai on 2016/9/25.
 */
public class LogSequence {
    private AtomicLong seq = new AtomicLong(0);

    public LogSequence(long seq) {
        this.seq.set(seq);
    }

    public long incrAndGet() {
        return  seq.incrementAndGet();
    }
}
