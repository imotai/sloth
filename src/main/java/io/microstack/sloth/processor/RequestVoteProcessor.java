package io.microstack.sloth.processor;

import io.grpc.stub.StreamObserver;
import io.microstack.sloth.core.ReplicateLogStatus;
import io.microstack.sloth.RequestVoteRequest;
import io.microstack.sloth.RequestVoteResponse;
import io.microstack.sloth.context.SlothContext;
import io.microstack.sloth.core.SlothOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by imotai on 2016/9/23.
 */
public class RequestVoteProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RequestVoteProcessor.class);
    private static final Logger event = LoggerFactory.getLogger("event");
    private SlothContext context;
    private SlothOptions options;
    private Map<Long, Long> votedFor = new HashMap<Long, Long>();

    public RequestVoteProcessor(SlothContext context,
                                SlothOptions options) {
        this.context = context;
        this.options = options;
    }

    public void process(RequestVoteRequest request,
                        StreamObserver<RequestVoteResponse> responseObserver) {
        context.getMutex().lock();
        try {
            Long idx = votedFor.get(request.getTerm());
            if (idx != null && idx != request.getCandidateId()) {
                logger.info("reject node {} for term {} voted for {}", request.getCandidateId(), request.getTerm(), idx);
                makeResponse(false, responseObserver);
                return;
            }
            if (context.getCurrentTerm() >= request.getTerm()
                    || !isLogMoreUpToDate(request)) {
                makeResponse(false, responseObserver);
                logger.info("reject node {} for log is not up to date , my term {} and request term {}",
                        request.getCandidateId(), context.getCurrentTerm(), request.getTerm());
                return;
            }
            votedFor.put(request.getTerm(), request.getCandidateId());
            makeResponse(true, responseObserver);
        } catch (Exception e) {
            logger.error("fail to process vote request ", e);
        } finally {
            context.getMutex().unlock();
        }
    }

    private void makeResponse(boolean voteGranted, StreamObserver<RequestVoteResponse> responseObserver) {
        assert context.getMutex().isHeldByCurrentThread();
        RequestVoteResponse.Builder builder = RequestVoteResponse.newBuilder();
        builder.setTerm(context.getCurrentTerm());
        builder.setVoteGranted(voteGranted);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private boolean isLogMoreUpToDate(RequestVoteRequest request) {
        assert context.getMutex().isHeldByCurrentThread();
        ReplicateLogStatus status = context.getLogStatus().get(context.getEndpoint());
        if (request.getLastLogTerm() < status.getLastLogTerm()) {
            logger.info("vote is reject for req log term {} my term {}", request.getLastLogTerm(), status.getLastLogTerm());
            return false;
        }
        if (request.getLastLogTerm() == status.getLastLogTerm()
                && request.getLastLogIndex() < status.getLastLogIndex()) {
            logger.info("vote is reject for req log index {} my index {}", request.getLastLogIndex(), status.getLastLogIndex());
            return false;
        }
        return true;
    }
}
