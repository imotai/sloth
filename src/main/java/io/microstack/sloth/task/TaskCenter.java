package io.microstack.sloth.task;

import io.microstack.sloth.RaftCore;
import io.microstack.sloth.common.SlothThreadFactory;
import io.microstack.sloth.context.SlothContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by imotai on 16/9/21.
 */
@Service
public class TaskCenter {
    private final static Logger logger = LoggerFactory.getLogger(RaftCore.class);
    private ScheduledFuture<?> electionTimeoutHandle = null;
    private ScheduledFuture<?> requestVoteTimeoutHandle = null;
    private ScheduledFuture<?> heartBeatHandle = null;
    private Random random;
    private SlothContext context;
    public TaskCenter(SlothContext context) {
        this.context = context;
        random = new Random(System.nanoTime());
    }

}
