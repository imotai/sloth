package io.microstack.sloth.monitor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by imotai on 2016/9/25.
 */
public class GCounter {
    public static final AtomicLong WRITE_COUNTER = new AtomicLong(0);
    public static final AtomicLong APPEND_LOG_COUNTER = new AtomicLong(0);
}
