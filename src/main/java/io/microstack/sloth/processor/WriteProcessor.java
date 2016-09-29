package io.microstack.sloth.processor;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import io.microstack.sloth.*;
import io.microstack.sloth.context.SlothContext;
import io.microstack.sloth.core.ReplicateLogStatus;
import io.microstack.sloth.core.SlothOptions;
import io.microstack.sloth.core.WriteTask;
import io.microstack.sloth.log.Binlogger;
import io.microstack.sloth.monitor.QpsRecorder;
import io.microstack.sloth.rpc.SlothStub;
import io.microstack.sloth.rpc.SlothStubPool;
import io.microstack.sloth.storage.DataStore;
import io.microstack.sloth.task.TaskManager;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by imotai on 2016/9/23.
 */
public class WriteProcessor {
    private static final Logger logger = LoggerFactory.getLogger(WriteProcessor.class);
    private static final Logger event = LoggerFactory.getLogger("event");
    private SlothContext context;
    private SlothOptions options;
    private Binlogger binlogger;
    private DataStore dataStore;
    private TaskManager taskManager;
    private SlothStubPool stubPool;
    private QpsRecorder.Counter counter;
    private LinkedList<WriteTask> tasks = new LinkedList<WriteTask>();
    public WriteProcessor(SlothContext context,
                          SlothOptions options,
                          Binlogger binlogger,
                          DataStore dataStore,
                          TaskManager taskManager,
                          SlothStubPool stubPool,
                          QpsRecorder.Counter counter) {
        this.context = context;
        this.options = options;
        this.binlogger = binlogger;
        this.dataStore = dataStore;
        this.taskManager = taskManager;
        this.stubPool = stubPool;
        this.counter = counter;
    }

    public void process(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        WriteTask task = new WriteTask(request, responseObserver, context.getWriteCond());
        task.setIndex(context.getSequence().incrAndGet());
        task.startWaitToWrite();
        context.getMutex().lock();
        task.buildEntry(context.getCurrentTerm());
        try {
            if (tasks.size() > options.getMaxTaskCount()
                    || context.getRole() != SlothNodeRole.kLeader) {
                makeResponse(RpcStatus.kRpcRejected, responseObserver);
                return;
            }
            tasks.add(task);
            while (tasks.peek() != null && !task.isDone() && !task.equals(tasks.peek())) {
                try {
                    task.getCondition().wait();
                } catch (InterruptedException e) {
                    logger.error("fail to wait task completed", e);
                    makeResponse(RpcStatus.kRpcError, responseObserver);
                    return;
                }
            }
            if (task.isDone()) {
                makeResponse(RpcStatus.kRpcOk, responseObserver);
                return;
            }
            task.endWaitToWrite();
            task.startWriteLog();
            TreeMap<Long, WriteTask> dataMap = batchWriteLog(task);
            scheduleReplicateLog(dataMap);
        } finally {
            if (context.getMutex().isHeldByCurrentThread()) {
                context.getMutex().unlock();
            }
        }
    }

    private TreeMap<Long, WriteTask> batchWriteLog(WriteTask last) {
        assert context.getMutex().isHeldByCurrentThread();
        List<Entry> entries = new ArrayList<Entry>();
        for (WriteTask task : tasks) {
            entries.add(task.getEntry());
            if (task.equals(last)) {
                break;
            }
        }

        TreeMap<Long, WriteTask> dataMap = new TreeMap<Long, WriteTask>();
        try {
            binlogger.batchWrite(entries);
        } catch (Exception e) {
            logger.error("fail to batch write log", e);
            for (int i = 0; i < tasks.size(); i++) {
                tasks.get(i).setDone(true);
                tasks.get(i).endWriteLog();
                StreamObserver<PutResponse> observer = tasks.get(i).getResponseObserver();
                makeResponse(RpcStatus.kRpcError, observer);
            }
            tasks.clear();
            context.getWriteCond().notify();
            return dataMap;
        }

        ReplicateLogStatus status = context.getLogStatus().get(context.getEndpoint());
        status.setLastLogTerm(context.getCurrentTerm());
        long oldIndex = status.getLastLogIndex();
        for (WriteTask task : tasks) {
            status.setLastLogIndex(task.getIndex());
            dataMap.put(task.getIndex(), task);
            if (task.equals(last)) {
                break;
            }
        }
        long newIndex = status.getLastLogIndex();
        logger.info("[WriteLog] update leader log index from {} to {}", oldIndex, newIndex);
        return dataMap;
    }

    private ListenableFuture<AppendEntriesResponse> sendAppendLogRequest(HostAndPort endpoint,
                                                                         long indexFrom, long indexTo, long leaderCommitIdx) {
        assert context.getMutex().isHeldByCurrentThread();
        ReplicateLogStatus status = context.getLogStatus().get(endpoint);
        if (status == null || !status.isMatched()) {
            logger.warn("[ReplicateLog]follower with endpoint {} is not ready", endpoint);
            return null;
        }
        SlothStub slothStub = stubPool.getByEndpoint(endpoint.toString());
        try {
            List<Entry> entries = binlogger.batchGet(indexFrom, indexTo);
            AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
            for (Entry entry : entries) {
                builder.addEntries(entry);
            }
            builder.setTerm(context.getCurrentTerm());
            builder.setLeaderIdx(options.getIdx());
            builder.setPreLogIndex(status.getLastLogIndex());
            builder.setPreLogTerm(status.getLastLogTerm());
            builder.setLeaderCommitIdx(leaderCommitIdx);
            ListenableFuture<AppendEntriesResponse> responseFuture = slothStub.getFstub().appendEntries(builder.build());
            return responseFuture;
        } catch (InvalidProtocolBufferException e) {
            logger.error("fail batch get from binlogger from {} to {} for endpoint", indexFrom, indexTo, endpoint, e);
            return null;
        }
    }

    private boolean checkReplicateLogResult(Map<HostAndPort, ListenableFuture<AppendEntriesResponse>> responses,
                                            ReplicateLogListener listener) {
        assert context.getMutex().isHeldByCurrentThread();
        int major = options.getEndpoints().size() / 2 + 1;
        int replicateLogSuccessCount = 1;
        boolean notified = false;
        Set<HostAndPort> doneNodes = new HashSet<HostAndPort>();
        ReplicateLogStatus leaderStatus = context.getLogStatus().get(context.getEndpoint());
        while (responses.size() > doneNodes.size()) {
            Iterator<Map.Entry<HostAndPort, ListenableFuture<AppendEntriesResponse>>> fit = responses.entrySet().iterator();
            while (fit.hasNext()) {
                if (!notified && replicateLogSuccessCount >= major) {
                    listener.onSuccess();
                    notified = true;
                }
                Map.Entry<HostAndPort, ListenableFuture<AppendEntriesResponse>> entry = fit.next();
                if (!entry.getValue().isDone() || doneNodes.contains(entry.getKey())) {
                    continue;
                }
                doneNodes.add(entry.getKey());
                final ReplicateLogStatus status = context.getLogStatus().get(entry.getKey());
                try {
                    final long indexFrom = status.getLastLogIndex() + 1;
                    final long indexTo = leaderStatus.getLastLogIndex();
                    AppendEntriesResponse response = entry.getValue().get();
                    if (response.getSuccess()) {
                        replicateLogSuccessCount++;
                        status.setLastLogTerm(context.getCurrentTerm());
                        status.setLastLogIndex(indexTo);
                        logger.info("[ReplicateLog] replicate log  {} from {} to {} ", entry.getKey(), indexFrom,
                                indexTo);
                    } else {
                        logger.error("[ReplicateLog] replicate log  {} from {} to {} failed ", entry.getKey(), indexFrom,
                                indexTo);
                    }

                } catch (Exception e) {
                    logger.error("fail to get response from {}", entry.getKey(), e);
                }
            }
        }
        if (!notified) {
            logger.info("fail to replicate log replicate count {}", replicateLogSuccessCount);
        }
        return notified;
    }


    private void scheduleReplicateLog(final TreeMap<Long, WriteTask> batchTask) {
        assert context.getMutex().isHeldByCurrentThread();
        if (context.getRole() != SlothNodeRole.kLeader) {
            return;
        }
        Iterator<Map.Entry<Long, WriteTask>> it = batchTask.entrySet().iterator();
        // trace sync log time
        while (it.hasNext()) {
            it.next().getValue().startSyncLog();
        }

        ReplicateLogStatus leaderStatus = context.getLogStatus().get(context.getEndpoint());
        final long preLogIndex = leaderStatus.getLastLogIndex();
        final long preLogTerm = leaderStatus.getLastLogTerm();
        final long commitIdx = leaderStatus.getCommitIndex();

        Map<HostAndPort, ListenableFuture<AppendEntriesResponse>> futures = new HashMap<HostAndPort, ListenableFuture<AppendEntriesResponse>>();
        for (int i = 0; i < options.getEndpoints().size(); i++) {
            final HostAndPort endpoint = options.getEndpoints().get(i);
            if (endpoint.equals(context.getEndpoint())) {
                continue;
            }
            final ReplicateLogStatus status = context.getLogStatus().get(endpoint);
            if (leaderStatus.getLastLogIndex() > status.getLastLogIndex()) {
                final long indexFrom = status.getLastLogIndex() + 1;
                final long indexTo = leaderStatus.getLastLogIndex();
                ListenableFuture<AppendEntriesResponse> response = sendAppendLogRequest(endpoint, indexFrom, indexTo, commitIdx);
                if (response == null) {
                    continue;
                }
                futures.put(endpoint, response);
            } else {
                logger.warn("leader last index {} node {} last index {}", leaderStatus.getLastLogIndex(),
                        endpoint, status.getLastLogIndex());
            }
        }
        boolean notified = checkReplicateLogResult(futures, new ReplicateLogListener() {
            @Override
            public void onSuccess() {
                Iterator<Map.Entry<Long, WriteTask>> it = batchTask.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<Long, WriteTask> entry = it.next();
                    entry.getValue().endSyncLog();
                    logger.info("[CommitLog] task #WaitToWrite {}ms #WriteLog {}ms #SyncLog {}ms #CommitToLocal {}ms",
                            entry.getValue().getWaitToWrite(),
                            entry.getValue().getWriteLogConsumed(),
                            entry.getValue().getSyncLogConsumed(),
                            entry.getValue().getCommitToLocalConsumed());
                }
                logger.info("[ReplicateLog] move log index to {} with term {}", preLogIndex, preLogTerm);
                scheduleCommit(preLogIndex, preLogTerm, commitIdx, batchTask);
            }
        });
        if (!notified) {
            failAllRequest(batchTask);
        }
        context.getWriteCond().notify();
    }

    private void scheduleCommit(long preLogIndex, long preLogTerm, long commitIdx,
                                TreeMap<Long, WriteTask> entries) {
        assert context.getMutex().isHeldByCurrentThread();
        if (commitIdx >= preLogIndex) {
            return;
        }
        long commitFrom = commitIdx + 1;
        long commitTo = preLogIndex;
        TreeMap<Long, Entry> batch = new TreeMap<Long, Entry>();
        for (long index = commitFrom; index <= commitTo; ++index) {
            if (entries.containsKey(index)) {
                entries.get(index).startCommit();
                batch.put(index, entries.get(index).getEntry());
            } else {
                try {
                    batch.put(index, binlogger.get(index));
                } catch (Exception e) {
                    logger.error("fail to get log with {}", index, e);
                    failAllRequest(entries);
                    return;
                }
            }
        }
        try {
            logger.info("[CommitLog] prepare to commit from {} to {} with batch size {}",
                    commitFrom, commitTo, batch.size());
            dataStore.batchWrite(batch);
            Iterator<Map.Entry<Long, WriteTask>> it = entries.entrySet().iterator();
            PutResponse response = PutResponse.newBuilder().setStatus(RpcStatus.kRpcOk).build();
            while (it.hasNext()) {
                Map.Entry<Long, WriteTask> entry = it.next();
                entry.getValue().endCommit();
                entry.getValue().setDone(true);
                logger.info("[CommitLog] task #WaitToWrite {}ms #WriteLog {}ms #SyncLog {}ms #CommitToLocal {}ms",
                        entry.getValue().getWaitToWrite(),
                        entry.getValue().getWriteLogConsumed(),
                        entry.getValue().getSyncLogConsumed(),
                        entry.getValue().getCommitToLocalConsumed());
                StreamObserver<PutResponse> observer = entry.getValue().getResponseObserver();
                counter.counter.incrementAndGet();
                observer.onNext(response);
                observer.onCompleted();
            }
            tasks.clear();
            logger.info("[CommitLog] commit log with index {} sucessfully", commitTo);
            ReplicateLogStatus leaderStatus = context.getLogStatus().get(context.getEndpoint());
            leaderStatus.setCommitIndex(commitTo);
            leaderStatus.setLastApplied(commitTo);
        } catch (RocksDBException e) {
            logger.error("fail batch write data store", e);
            failAllRequest(entries);
        }

    }

    private void failAllRequest(TreeMap<Long, WriteTask> entries) {
        assert context.getMutex().isHeldByCurrentThread();
        Iterator<Map.Entry<Long, WriteTask>> it = entries.entrySet().iterator();
        PutResponse response = PutResponse.newBuilder().setStatus(RpcStatus.kRpcError).build();
        while (it.hasNext()) {
            Map.Entry<Long, WriteTask> entry = it.next();
            entry.getValue().setDone(true);
            StreamObserver<PutResponse> observer = entry.getValue().getResponseObserver();
            observer.onNext(response);
            observer.onCompleted();
        }
        tasks.clear();
    }

    private void makeResponse(RpcStatus status, StreamObserver<PutResponse> responseObserver) {
        PutResponse.Builder builder = PutResponse.newBuilder();
        builder.setStatus(status);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    interface ReplicateLogListener {
        void onSuccess();
    }

}
