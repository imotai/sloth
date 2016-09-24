package io.microstack.sloth.processor;

import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import io.microstack.sloth.*;
import io.microstack.sloth.common.GThreadPool;
import io.microstack.sloth.context.SlothContext;
import io.microstack.sloth.core.ReplicateLogStatus;
import io.microstack.sloth.core.SlothOptions;
import io.microstack.sloth.log.Binlogger;
import io.microstack.sloth.storage.DataStore;
import io.microstack.sloth.task.TaskManager;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeMap;

/**
 * Created by imotai on 16/9/21.
 */

public class AppendLogProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AppendLogProcessor.class);
    private static final Logger event = LoggerFactory.getLogger("event");
    private SlothContext context;
    private SlothOptions options;
    private Binlogger binlogger;
    private DataStore dataStore;
    private TaskManager taskManager;
    public AppendLogProcessor(SlothContext context,
                              SlothOptions options,
                              Binlogger binlogger,
                              DataStore dataStore,
                              TaskManager taskManager) {
        this.context = context;
        this.options = options;
        this.binlogger = binlogger;
        this.dataStore = dataStore;
        this.taskManager = taskManager;
    }


    /**
     * if log matches return true, otherwise return false.
     *
     * */
    public void process(final AppendEntriesRequest request,
                        StreamObserver<AppendEntriesResponse> responseObserver) {
        context.getMutex().lock();
        try {
            HostAndPort endpoint = options.getEndpoints().get(options.getIdx());
            ReplicateLogStatus status = context.getLogStatus().get(endpoint);
            AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder();
            if (request.getTerm() < context.getCurrentTerm()) {
                makeOkResponse(responseObserver, false);
                return;
            }
            if (context.isCandidate()) {
                processForCandidate(request, responseObserver);
            }else if (context.isFollower()) {
                processForFollower(request, responseObserver);
            }else {
                processForLeader(request, responseObserver);
            }
        } catch (Throwable t) {
            logger.error("fail to process append log request", t);
            makeErrorResponse(responseObserver);
        } finally {
            if (context.getMutex().isHeldByCurrentThread()) {
                context.getMutex().unlock();
            }
        }
    }

    /**
     * change to follower , match log and apply log
     *
     * */
    private void processForCandidate(final AppendEntriesRequest request,
                                     StreamObserver<AppendEntriesResponse> responseObserver) {
        assert context.getMutex().isHeldByCurrentThread();
        becomeToFollower((int)request.getLeaderIdx(), request.getTerm());
        processForFollower(request, responseObserver);
    }

    private void processForFollower(final AppendEntriesRequest request,
                                    StreamObserver<AppendEntriesResponse> responseObserver) {
        assert context.getMutex().isHeldByCurrentThread();
        try {
            if (request.getTerm() > context.getCurrentTerm()) {
                logger.info("[AppendLog] update my term to {} from leader {}", request.getTerm(), request.getLeaderIdx());
                context.setCurrentTerm(request.getTerm());
            }
            if (request.getLeaderIdx() != context.getLeaderIdx()) {
                logger.info("[AppendLog] update leader idx to {}", request.getLeaderIdx());
                context.setLeaderIdx((int)request.getLeaderIdx());
            }
            taskManager.resetDelayTask(TaskManager.TaskType.kElectionTask);
            if (isMatchLog(request)) {
                boolean ok = true;
                if (request.getEntriesList() != null
                        && request.getEntriesList().size() > 0) {
                    ok = handleAppendLog(request);
                }
                makeOkResponse(responseObserver, ok);
            }else {
                makeOkResponse(responseObserver, false);
            }
        } catch (Exception e) {
            logger.error("fail to match log", e);
            makeErrorResponse(responseObserver);
        }
    }


    private void processForLeader(final AppendEntriesRequest request,
                                  StreamObserver<AppendEntriesResponse> responseObserver) {
        assert context.getMutex().isHeldByCurrentThread();
        try {
            if (request.getTerm() > context.getCurrentTerm()) {
                taskManager.stopTask(TaskManager.TaskType.kHeartBeatTask);
                becomeToFollower((int)request.getLeaderIdx(), request.getTerm());
                processForFollower(request, responseObserver);
            }
        } catch (Exception e) {
            logger.error("fail to process append entry", e);
        }
    }
    /**
     * become follower
     *
     * */
    private void becomeToFollower(int leaderIdx, long term) {
        assert context.getMutex().isHeldByCurrentThread();
        long oldTerm = context.getCurrentTerm();
        event.info("#Role {} to {} #Term {} to {} #Leader {}",
                context.getRole(), SlothNodeRole.kFollower, oldTerm, term, leaderIdx);
        context.resetToFollower(leaderIdx, term);
    }

    private boolean isMatchLog(final AppendEntriesRequest request) throws Exception {
        assert context.getMutex().isHeldByCurrentThread();
        ReplicateLogStatus status = context.getLogStatus().get(context.getEndpoint());
        if (status.getLastLogIndex() == request.getPreLogIndex()
                && status.getLastLogTerm() == request.getPreLogTerm()) {
            status.setMatched(true);
            return true;
        }

        if (status.getLastLogIndex() < request.getPreLogIndex()) {
            logger.info("[LogMatch] log match fails for my last log index {}  and leader log index {}",
                    status.getLastLogIndex(), request.getPreLogIndex());
            return false;
        }

        if (request.getPreLogIndex() < status.getLastLogIndex()) {
            if (request.getPreLogIndex() <= 0) {
                logger.info("[LogMatch] clean binlogger from index 1");
                binlogger.removeLog(1);
                return true;
            } else {
                Entry entry = binlogger.get(request.getPreLogIndex());
                if (entry.getTerm() == request.getPreLogTerm()) {
                    logger.info("[LogMatch] clean binlogger from index {}", request.getPreLogIndex() + 1);
                    binlogger.removeLog(request.getPreLogIndex() + 1);
                    status.setLastLogIndex(binlogger.getPreLogIndex());
                    status.setLastLogTerm(binlogger.getPreLogTerm());
                    return true;
                }
                logger.info("[LogMatch] log match fails for my last log term {}  and leader log term {}",
                        entry.getTerm(), request.getPreLogTerm());
            }
            return false;
        }
        return false;
    }

    private void makeErrorResponse(StreamObserver<AppendEntriesResponse> responseObserver) {
        assert context.getMutex().isHeldByCurrentThread();
        AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder();
        builder.setTerm(context.getCurrentTerm());
        builder.setSuccess(false);
        builder.setStatus(RpcStatus.kRpcError);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private void makeOkResponse(StreamObserver<AppendEntriesResponse> responseObserver, boolean success) {
        assert context.getMutex().isHeldByCurrentThread();
        AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder();
        builder.setTerm(context.getCurrentTerm());
        builder.setSuccess(success);
        builder.setStatus(RpcStatus.kRpcOk);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private boolean handleAppendLog(AppendEntriesRequest request) {
        assert context.getMutex().isHeldByCurrentThread();
        List<Entry> entries = request.getEntriesList();
        if (entries == null || entries.size() <= 0) {
            return true;
        }
        ReplicateLogStatus status = context.getLogStatus().get(context.getEndpoint());
        try {
            List<Long> ids = binlogger.batchWrite(entries);
            assert ids.size() == entries.size();
            status.setLastLogIndex(ids.get(ids.size() - 1));
            status.setLastLogTerm(request.getTerm());
            final long commitIdx = request.getLeaderCommitIdx() > status.getLastLogIndex() ? request.getLeaderCommitIdx() : status.getLastLogIndex();
            status.setCommitIndex(commitIdx);
            final long appliedIdx = status.getLastApplied();
            GThreadPool.getInstance().execute(new Runnable() {
                @Override
                public void run() {
                    schedApplyLog(appliedIdx, commitIdx);
                }
            });
            logger.info("[AppendLog] append log to index {}", status.getLastLogIndex());
            return true;
        } catch (RocksDBException e) {
            logger.error("fail to batch write entry", e);
            return false;
        }
    }

    private void schedApplyLog(long appliedIdx, long commitIdx) {
        try {
            if (appliedIdx >= commitIdx) {
                return;
            }
            List<Entry> entries = binlogger.batchGet(appliedIdx+1, commitIdx);
            TreeMap<Long, Entry> datas = new TreeMap<Long, Entry>();
            for (Entry e : entries) {
                datas.put(e.getLogIndex(), e);
            }
            //TODO retry
            dataStore.batchWrite(datas);
            context.getMutex().lock();
            ReplicateLogStatus status = context.getLogStatus().get(context.getEndpoint());
            if (status.getLastApplied() > appliedIdx) {
                return;
            }
            status.setLastApplied(commitIdx);
            logger.info("[BgApplyLog] apply log to {}", commitIdx);
        } catch (InvalidProtocolBufferException e) {
            logger.error("fail to batch get from binlogger ", e);
        } catch (RocksDBException e) {
            logger.error("fail to batch write", e);
        } finally {
            if (context.getMutex().isHeldByCurrentThread()) {
                context.getMutex().unlock();
            }
        }
    }
}
