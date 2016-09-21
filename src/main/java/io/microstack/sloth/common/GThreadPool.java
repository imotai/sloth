package io.microstack.sloth.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Created by imotai on 16/9/21.
 */
public class GThreadPool {
    private static final Logger status = LoggerFactory.getLogger("status");
    private static final GThreadPool pool = new GThreadPool();
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10,
                           0L, TimeUnit.MILLISECONDS,
                           new LinkedBlockingQueue<Runnable>(), new SlothThreadFactory("gpool"));

    private GThreadPool() {}

    public Executor getRawExecutor() {
        return executor;
    }

    public static GThreadPool getInstance() {
        return pool;
    }

    public void execute(Runnable task) {
        executor.execute(task);
    }

    private void logPoolStatus() {
        status.info("#GThreadPool Status# queue pending size {}, active thread size {} , max thread size {}",
                executor.getQueue().size(),
                executor.getActiveCount(),
                executor.getMaximumPoolSize());
        GSchedThreadPool.getInstance().schedule(new Runnable() {
            @Override
            public void run() {
                pool.logPoolStatus();
            }
        }, 5000);
    }
}
