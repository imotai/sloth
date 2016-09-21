package io.microstack.sloth.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by imotai on 16/9/21.
 */
public class GSchedThreadPool {
    private static final Logger logger = LoggerFactory.getLogger(GSchedThreadPool.class);
    private static final GSchedThreadPool pool = new GSchedThreadPool();
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(5, new SlothThreadFactory("gsched"));
    private GSchedThreadPool() {}

    public static GSchedThreadPool getInstance() {
        return pool;
    }

    public void schedule(Runnable task, long delay) {
        executor.schedule(task, delay, TimeUnit.MILLISECONDS);
    }


}
