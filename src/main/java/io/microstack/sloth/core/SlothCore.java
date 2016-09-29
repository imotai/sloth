package io.microstack.sloth.core;

import io.grpc.stub.StreamObserver;
import io.microstack.sloth.*;
import io.microstack.sloth.context.SlothContext;
import io.microstack.sloth.log.Binlogger;
import io.microstack.sloth.monitor.QpsRecorder;
import io.microstack.sloth.processor.AppendLogProcessor;
import io.microstack.sloth.processor.WriteProcessor;
import io.microstack.sloth.processor.RequestVoteProcessor;
import io.microstack.sloth.rpc.SlothStubPool;
import io.microstack.sloth.storage.DataStore;
import io.microstack.sloth.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by imotai on 2016/9/23.
 */
@Service
public class SlothCore {
    private static final Logger logger = LoggerFactory.getLogger(SlothCore.class);
    // spring managed beans
    @Autowired
    private SlothOptions options;
    @Autowired
    private Binlogger binlogger;
    @Autowired
    private DataStore dataStore;
    @Autowired
    private SlothStubPool stubPool;
    @Autowired
    private QpsRecorder qpsCounter;
    // self managed beans
    private SlothContext context;
    private AppendLogProcessor appendLogProcessor;
    private RequestVoteProcessor requestVoteProcessor;
    private WriteProcessor putProcessor;
    private TaskManager taskManager;

    public void init() {
        // init context
        context = new SlothContext(binlogger, dataStore, options);
        context.setEndpoint(options.getEndpoints().get(options.getIdx()));
        context.resetToFollower(-1, binlogger.getPreLogTerm());


        // init task manager
        taskManager = new TaskManager(context, options, stubPool, binlogger);
        // init processor
        appendLogProcessor = new AppendLogProcessor(context, options, binlogger, dataStore, taskManager);
        requestVoteProcessor = new RequestVoteProcessor(context, options);
        putProcessor = new WriteProcessor(context, options, binlogger, dataStore, taskManager,
                stubPool, qpsCounter.register("writeqps"));
        // add election timeout task
        taskManager.resetDelayTask(TaskManager.TaskType.kElectionTask);
        logger.info("init slot core with log index {} and term {}", binlogger.getPreLogIndex(),
                binlogger.getPreLogTerm());
    }

    public void requestVote(RequestVoteRequest request,
                            StreamObserver<RequestVoteResponse> responseObserver) {
        requestVoteProcessor.process(request, responseObserver);
    }

    public void appendLogEntries(final AppendEntriesRequest request,
                                 StreamObserver<AppendEntriesResponse> responseObserver) {
        appendLogProcessor.process(request, responseObserver);
    }

    public void put(final PutRequest request, StreamObserver<PutResponse> responseObserver) {
        putProcessor.process(request, responseObserver);
    }

    public SlothContext getContext() {
        return context;
    }

}
