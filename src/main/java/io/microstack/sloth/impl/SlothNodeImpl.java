package io.microstack.sloth.impl;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.microstack.sloth.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SlothNodeImpl extends SlothNodeGrpc.SlothNodeImplBase {
    private Logger logger = LoggerFactory.getLogger(SlothNodeImpl.class);
    @Autowired
    private SlothOptions options;
    @Autowired
    private RaftCore core;
    private Server server;
    private Executor executor = Executors.newScheduledThreadPool(10, new NodeThreadFactory("slothnode"));

    public void start() throws IOException {
        core.start();
        server = ServerBuilder.forPort(options.getEndpoints().get(options.getIdx()).getPort()).addService(this).executor(executor).build();
        server.start();
        logger.info("start sloth node with port {} successfully", options.getEndpoints().get(options.getIdx()).getPort());
    }

    @Override
    public void requestVote(RequestVoteRequest request,
                            StreamObserver<RequestVoteResponse> responseObserver) {
        core.requestVote(request, responseObserver);
    }

    @Override
    public void appendEntries(AppendEntriesRequest request,
                              StreamObserver<AppendEntriesResponse> responseObserver) {
        core.appendLogEntries(request, responseObserver);
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        core.put(request, responseObserver);
    }

    @Override
    public void getClusterStatus(GetClusterStatusRequest request,
                                 StreamObserver<GetClusterStatusResponse> responseObserver) {
        // TODO Auto-generated method stub
        super.getClusterStatus(request, responseObserver);
    }

    class NodeThreadFactory implements ThreadFactory {
        private AtomicInteger counter = new AtomicInteger(0);
        private String prefix;

        public NodeThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        public Thread newThread(Runnable r) {
            int index = counter.incrementAndGet();
            StringBuilder name = new StringBuilder();
            name.append(prefix).append("-").append(options.getIdx()).append("-").append(index);
            return new Thread(r, name.toString());
        }
    }

}
