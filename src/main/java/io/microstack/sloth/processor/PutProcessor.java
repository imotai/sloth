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
import io.microstack.sloth.rpc.SlothStub;
import io.microstack.sloth.rpc.SlothStubPool;
import io.microstack.sloth.storage.DataStore;
import io.microstack.sloth.task.TaskManager;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by imotai on 2016/9/23.
 */
public class PutProcessor {
    private static final Logger logger = LoggerFactory.getLogger(PutProcessor.class);
    private static final Logger event = LoggerFactory.getLogger("event");
    private SlothContext context;
    private SlothOptions options;
    private Binlogger binlogger;
    private DataStore dataStore;
    private TaskManager taskManager;
    private SlothStubPool stubPool;
    private List<WriteTask> tasks = new LinkedList<WriteTask>();
    public PutProcessor(SlothContext context,
                        SlothOptions options,
                        Binlogger binlogger,
                        DataStore dataStore,
                        TaskManager taskManager,
                        SlothStubPool stubPool) {
        this.context = context;
        this.options = options;
        this.binlogger = binlogger;
        this.dataStore = dataStore;
        this.taskManager = taskManager;
        this.stubPool = stubPool;
    }

    public void process(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        WriteTask task = new WriteTask(request, responseObserver, context.getWriteCond());
        task.startWaitToWrite();
        context.getMutex().lock();
        try {
            if (tasks.size() > options.getMaxTaskCount()) {
                makeResponse(RpcStatus.kRpcRejected, responseObserver);
                return;
            }
            tasks.add(task);
            while (tasks.size() > 0 && !task.isDone() && !task.equals(tasks.get(0))) {
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
            TreeMap<Long, WriteTask> dataMap = batchWriteLog();
        } finally {
            if (context.getMutex().isHeldByCurrentThread()) {
                context.getMutex().unlock();
            }
        }
    }

    private TreeMap<Long, WriteTask> batchWriteLog() {
        assert context.getMutex().isHeldByCurrentThread();
        List<Entry> entries = new ArrayList<Entry>();
        for (WriteTask task : tasks) {
            Entry entry = Entry.newBuilder().setTerm(context.getCurrentTerm())
                    .setValue(task.getRequest().getValue())
                    .setKey(task.getRequest().getKey())
                    .build();
            entries.add(entry);
            task.setEntry(entry);
            task.startWriteLog();
        }
        TreeMap<Long, WriteTask> dataMap = new TreeMap<Long, WriteTask>();
        List<Long> ids = null;
        try {
            ids = binlogger.batchWrite(entries);
        } catch (Exception e) {
            ids = null;
            logger.error("fail to batch write log", e);
        }
        if (ids == null) {
            for (int i = 0; i < tasks.size(); i++) {
                tasks.get(i).setDone(true);
                tasks.get(i).endWriteLog();
                StreamObserver<PutResponse> observer = tasks.get(i).getResponseObserver();
                makeResponse(RpcStatus.kRpcError, observer);
            }
            tasks.clear();
            context.getWriteCond().notify();
            return dataMap;
        } else {
            ReplicateLogStatus status = context.getLogStatus().get(context.getEndpoint());
            status.setLastLogTerm(context.getCurrentTerm());
            for (int i = 0; i < ids.size() && i < tasks.size(); i++) {
                tasks.get(i).setIndex(ids.get(i));
                tasks.get(i).endWriteLog();
                status.setLastLogIndex(ids.get(i));
                dataMap.put(ids.get(i), tasks.get(i));
            }
            return dataMap;
        }
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

    private void checkReplicateLogResult(Map<HostAndPort, ListenableFuture<AppendEntriesResponse>> responses,
                                            ReplicateLogListener listener) {
        assert context.getMutex().isHeldByCurrentThread();
        int major = options.getEndpoints().size() / 2 + 1;
        int replicateLogSuccessCount = 1;
        boolean notified = false;
        Set<HostAndPort> doneNodes = new HashSet<HostAndPort>();
        ReplicateLogStatus leaderStatus = context.getLogStatus().get(context.getEndpoint());
        while (responses.size() > doneNodes.size()) {
            if (!notified && replicateLogSuccessCount >= major) {
                listener.onSuccess();
                notified = true;
            }
            Iterator<Map.Entry<HostAndPort, ListenableFuture<AppendEntriesResponse>>> fit = responses.entrySet().iterator();
            while (fit.hasNext()) {
                Map.Entry<HostAndPort, ListenableFuture<AppendEntriesResponse>> entry = fit.next();
                if (!entry.getValue().isDone() || doneNodes.contains(entry.getKey())) {
                    continue;
                }
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
                    }else {
                        logger.error("[ReplicateLog] replicate log  {} from {} to {} failed ", entry.getKey(), indexFrom,
                                indexTo);
                    }
                    doneNodes.add(entry.getKey());
                } catch (Exception e) {
                    logger.error("fail to get response from {}", entry.getKey(), e);
                    doneNodes.add(entry.getKey());
                }
            }
        }
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
            final ReplicateLogStatus status = context.getLogStatus().get(endpoint);
            if (leaderStatus.getLastLogIndex() > status.getLastLogIndex()) {
                final long indexFrom = status.getLastLogIndex() + 1;
                final long indexTo = leaderStatus.getLastLogIndex();
                ListenableFuture<AppendEntriesResponse> response = sendAppendLogRequest(endpoint, indexFrom, indexTo, commitIdx);
                if (response == null) {
                    continue;
                }
                futures.put(endpoint, response);
            }
        }
        checkReplicateLogResult(futures, new ReplicateLogListener() {
            @Override
            public void onSuccess() {
                Iterator<Map.Entry<Long, WriteTask>> it = batchTask.entrySet().iterator();
                while (it.hasNext()) {
                    it.next().getValue().endSyncLog();
                }
                logger.info("[ReplicateLog] move log index to {} with term {}", preLogIndex, preLogTerm);
                scheduleCommit(preLogIndex, preLogTerm, commitIdx, batchTask);
            }
        });
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
                }
            }
        }
        try {
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
                observer.onNext(response);
                observer.onCompleted();
            }
            tasks.clear();
            logger.info("[CommitLog] commit log with index {} sucessfully", commitTo);
            ReplicateLogStatus leaderStatus = context.getLogStatus().get(context.getEndpoint());
            leaderStatus.setCommitIndex(commitTo);
            context.getWriteCond().notify();
        } catch (RocksDBException e) {
            logger.error("fail batch write data store", e);
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
            context.getWriteCond().notify();
        }

    }


    private void makeResponse(RpcStatus status, StreamObserver<PutResponse> responseObserver) {
        PutResponse.Builder builder =  PutResponse.newBuilder();
        builder.setStatus(status);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    interface ReplicateLogListener {
        void onSuccess();
    }

}
