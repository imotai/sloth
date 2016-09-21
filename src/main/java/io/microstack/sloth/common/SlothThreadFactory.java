package io.microstack.sloth.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by imotai on 16/9/21.
 */
public class SlothThreadFactory implements ThreadFactory {
    private AtomicInteger counter = new AtomicInteger(0);
    private String prefix;
    public SlothThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        int index = counter.incrementAndGet();
        StringBuilder name = new StringBuilder();
        name.append(prefix).append("-").append(index);
        return new Thread(r, name.toString());
    }
}
