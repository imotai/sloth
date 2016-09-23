package io.microstack.sloth.core;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.microstack.sloth.*;
import io.microstack.sloth.common.SlothThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by imotai on 2016/9/23.
 */
@Service
public class SlothNode extends SlothNodeGrpc.SlothNodeImplBase {
    private static final Logger logger = LoggerFactory.getLogger(SlothNode.class);
    @Autowired
    private SlothOptions options;
    @Autowired
    private SlothCore core;
    private Server server;
    private Executor executor = Executors.newScheduledThreadPool(10, new SlothThreadFactory("rpc-pool"));

    public void start() throws IOException {
        core.init();
        server = ServerBuilder.forPort(options.getEndpoints().get(options.getIdx())
                .getPort()).addService(this).executor(executor).build();
        server.start();
        logger.info("start sloth node with port {} successfully", options.getEndpoints().get(options.getIdx()).getPort());
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        core.requestVote(request, responseObserver);
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        core.appendLogEntries(request, responseObserver);
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        core.put(request, responseObserver);
    }

    @Override
    public void getClusterStatus(GetClusterStatusRequest request, StreamObserver<GetClusterStatusResponse> responseObserver) {
        super.getClusterStatus(request, responseObserver);
    }
}
