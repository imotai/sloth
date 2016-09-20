package io.microstack.sloth;

import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class RaftCore {

    private final static Logger logger = LoggerFactory.getLogger(RaftCore.class);
    private final static Logger event = LoggerFactory.getLogger("event");
    private final static Logger status = LoggerFactory.getLogger("status");
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition writeCond = mutex.newCondition();
    @Autowired
    private SlothOptions options;
    private int leaderIdx;
    private long currentTerm;
    private SlothNodeRole role;
    private boolean running;
    private Map<HostAndPort, ReplicateLogStatus> logStatus = new HashMap<HostAndPort, ReplicateLogStatus>();
    // executor for checking timeouto
    private ScheduledExecutorService schedPool = Executors.newScheduledThreadPool(8, new RaftThreadFactory("raftcore.sched"));
    private Executor directPool = Executors.newFixedThreadPool(4, new RaftThreadFactory("raftcore.direct"));
    private Executor applyLogPool = Executors.newFixedThreadPool(1, new RaftThreadFactory("raftcore.applylog"));
    @Autowired
    private SlothStubPool slothStubPool;
    private ScheduledFuture<?> electionTimeoutHandle = null;
    private ScheduledFuture<?> requestVoteTimeoutHandle = null;
    private ScheduledFuture<?> heartBeatHandle = null;
    private Random random;
    private int voteCount = 0;
    @Autowired
    private Binlogger binlogger;
    @Autowired
    private DataStore dataStore;
    private AtomicBoolean electing = new AtomicBoolean(false);
    private Map<Long, Integer> votedFor = new HashMap<Long, Integer>();
    private List<WriteTask> tasks = new LinkedList<WriteTask>();

    public RaftCore() {
        currentTerm = 0;
    }

    public SlothOptions getOptions() {
        return options;
    }

    public void setOptions(SlothOptions options) {
        this.options = options;
    }

    public void setSlothStubPool(SlothStubPool slothStubPool) {
        this.slothStubPool = slothStubPool;
    }

    public int getLeaderIdx() {
        return leaderIdx;
    }

    public void start() {
        try {
            mutex.lock();
            if (running) {
                logger.warn("sloth node {} has been running", options.getEndpoints().get(options.getIdx()));
                return;
            }
            random = new Random(System.nanoTime());
        } finally {
            mutex.unlock();
        }
        startElection();
        logger.info("start raft core with idx {}", options.getIdx());
    }

    public void startElection() {
        mutex.lock();
        try {
            electing.set(true);
            becomeToFollower(-1, 0);
        } finally {
            mutex.unlock();
        }
    }

    public void stopElection() {
        electing.set(false);
        mutex.lock();
        try {
            stopHeartBeat();
            stopElectionTimeoutCheck();
            stopVoteTimeoutCheck();
        } finally {
            mutex.unlock();
        }
    }

    public void requestVote(RequestVoteRequest request,
                            StreamObserver<RequestVoteResponse> responseObserver) {
        try {
            mutex.lock();
            boolean granted = false;
            if (request.getTerm() > currentTerm) {
                granted = true;
                if (votedFor.containsKey(request.getTerm())) {
                    int candidateId = votedFor.get(request.getTerm());
                    if (candidateId != request.getCandidateId()) {
                        granted = false;
                    }
                }
            }
            if (granted) {
                votedFor.put(request.getTerm(), (int) request.getCandidateId());
            }
            RequestVoteResponse response = RequestVoteResponse.newBuilder().setTerm(currentTerm)
                    .setVoteGranted(granted).setStatus(RpcStatus.kRpcOk).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } finally {
            mutex.unlock();
        }
    }

    public ReplicateLogStatus getNodeStatus(HostAndPort endpoint) {
        mutex.lock();
        try {
            return logStatus.get(endpoint);
        } finally {
            mutex.unlock();
        }
    }

    public void appendLogEntries(final AppendEntriesRequest request,
                                 StreamObserver<AppendEntriesResponse> responseObserver) {
        try {
            mutex.lock();
            boolean success = false;
            // become follower
            // 1. clear replicate log task when I am leader
            // 2. clear vote timeout check when I am candidate
            // 3. reset election timeout check
            // 4. change role to Follower
            // 5. update term to latest
            logger.debug("[AppendEntry] from leader {} with term {} and my term {}", request.getLeaderIdx(), request.getTerm(), currentTerm);
            if (request.getTerm() > currentTerm) {
                SlothNodeRole oldRole = role;
                leaderIdx = (int) request.getLeaderIdx();
                becomeToFollower(leaderIdx, request.getTerm());
                success = true;
                logger.info("[AppendEntry] become follower for term update to {}", currentTerm);
                event.debug("change role from {} to {} with term {}", oldRole, role, currentTerm);
            } else if (request.getTerm() == currentTerm) {
                success = true;
                leaderIdx = (int) request.getLeaderIdx();
                if (role == SlothNodeRole.kCandidate) {
                    becomeToFollower(leaderIdx, request.getTerm());
                } else {
                    role = SlothNodeRole.kFollower;
                    resetElectionTimeout();
                    // skip heartbeat
                    if (request.getLeaderCommitIdx() >= 0) {
                        success = handleMatchIndexOnFollower(request.getPreLogIndex(), request.getPreLogTerm());
                        if (success
                                && request.getEntriesList() != null
                                && request.getEntriesList().size() > 0) {
                            success = handleAppendLog(request);
                            HostAndPort endpoint = options.getEndpoints().get(options.getIdx());
                            final ReplicateLogStatus status = logStatus.get(endpoint);
                            if (success && request.getLeaderCommitIdx() > status.getCommitIndex()) {
                                applyLogPool.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        bgApplyLog(status.getCommitIndex() + 1, request.getLeaderCommitIdx());
                                    }
                                });
                            }
                        }
                    }
                }
                request.toBuilder().setTerm(currentTerm);
            } else {
                success = false;
            }
            AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm).setSuccess(success).setStatus(RpcStatus.kRpcOk).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } finally {
            mutex.unlock();
        }
    }

    private boolean handleAppendLog(AppendEntriesRequest request) {
        assert mutex.isHeldByCurrentThread();
        List<Entry> entries = request.getEntriesList();
        if (entries == null || entries.size() <= 0) {
            return true;
        }
        HostAndPort endpoint = options.getEndpoints().get(options.getIdx());
        ReplicateLogStatus status = logStatus.get(endpoint);
        try {
            List<Long> ids = binlogger.batchWrite(entries);
            assert ids.size() == entries.size();
            status.setLastLogIndex(ids.get(ids.size() - 1));
            status.setLastLogTerm(request.getTerm());
            logger.info("[AppendLog] append log to index {}", status.getLastLogIndex());
            return true;
        } catch (RocksDBException e) {
            logger.error("fail to batch write entry", e);
            return false;
        }
    }

    private void bgApplyLog(long commitFrom, long commitTo) {
        if (commitFrom >= commitTo) {
            return;
        }
        try {
            List<Entry> entries = binlogger.batchGet(commitFrom, commitTo);
            TreeMap<Long, Entry> datas = new TreeMap<Long, Entry>();
            for (Entry e : entries) {
                datas.put(e.getLogIndex(), e);
            }
            //TODO retry
            dataStore.batchWrite(datas);
            mutex.lock();
            try {
                HostAndPort endpoint = options.getEndpoints().get(options.getIdx());
                ReplicateLogStatus status = logStatus.get(endpoint);
                status.setCommitIndex(commitTo);
            } finally {
                mutex.unlock();
            }
            logger.info("[BgApplyLog] apply log to {}", commitTo);
        } catch (InvalidProtocolBufferException e) {
            logger.error("fail to batch get from binlogger ", e);
        } catch (RocksDBException e) {
            logger.error("fail to batch write", e);
        }
    }

    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        mutex.lock();
        try {
            WriteTask task = new WriteTask(request, responseObserver, writeCond);
            tasks.add(task);
            task.startWriteLog();
            task.startWaitToWrite();
            while (tasks.size() > 0 && !task.isDone() && !task.equals(tasks.get(0))) {
                try {
                    task.getCondition().wait();
                } catch (InterruptedException e) {
                    logger.error("fail to wait task", e);
                }
            }

            if (task.isDone()) {
                return;
            }
            task.endWaitToWrite();
            TreeMap<Long, WriteTask> entries = batchWriteLog();
            scheduleReplicateLog(entries);
        } catch (Exception e) {

        } finally {
            mutex.unlock();
        }
    }

    private TreeMap<Long, WriteTask> batchWriteLog() throws RocksDBException {
        assert mutex.isHeldByCurrentThread();
        List<Entry> entries = new ArrayList<Entry>();
        for (WriteTask task : tasks) {
            Entry entry = Entry.newBuilder().setTerm(currentTerm).setValue(task.getRequest().getValue()).setKey(task.getRequest().getKey()).build();
            entries.add(entry);
            task.setEntry(entry);
        }
        TreeMap<Long, WriteTask> datas = new TreeMap<Long, WriteTask>();
        List<Long> ids = null;
        try {
            ids = binlogger.batchWrite(entries);
        } catch (Exception e) {
            ids = null;
            logger.error("fail to batch write log", e);
        }
        if (ids == null) {
            for (int i = 0; i < tasks.size(); i++) {
                PutResponse response = PutResponse.newBuilder().setStatus(RpcStatus.kRpcError).build();
                tasks.get(i).setDone(true);
                StreamObserver<PutResponse> observer = tasks.get(i).getResponseObserver();
                observer.onNext(response);
                observer.onCompleted();
            }
            tasks.clear();
            writeCond.notify();
            return datas;
        } else {
            HostAndPort endpoint = options.getEndpoints().get(options.getIdx());
            ReplicateLogStatus status = logStatus.get(endpoint);
            status.setLastLogTerm(currentTerm);
            for (int i = 0; i < ids.size() && i < tasks.size(); i++) {
                tasks.get(i).setIndex(ids.get(i));
                tasks.get(i).endWriteLog();
                status.setLastLogIndex(ids.get(i));
                datas.put(ids.get(i), tasks.get(i));
            }
            return datas;
        }
    }


    private void scheduleReplicateLog(TreeMap<Long, WriteTask> batchTask) {
        assert mutex.isHeldByCurrentThread();
        if (role != SlothNodeRole.kLeader) {
            return;
        }
        Iterator<Map.Entry<Long, WriteTask>> it = batchTask.entrySet().iterator();
        while (it.hasNext()) {
            it.next().getValue().startSyncLog();
        }
        HostAndPort leaderEndpoint = options.getEndpoints().get(options.getIdx());
        ReplicateLogStatus leaderStatus = logStatus.get(leaderEndpoint);
        long preLogIndex = leaderStatus.getLastLogIndex();
        long preLogTerm = leaderStatus.getLastLogTerm();
        long commitIdx = dataStore.getCommitIdx();
        int major = options.getEndpoints().size() / 2 + 1;
        final CountDownLatch finishLatch = new CountDownLatch(major - 1);
        for (int i = 0; i < options.getEndpoints().size(); i++) {
            if (i == options.getIdx()) {
                continue;
            }
            final HostAndPort endpoint = options.getEndpoints().get(i);
            final ReplicateLogStatus status = logStatus.get(endpoint);
            if (status == null
                    || !status.isMatched()) {
                logger.warn("[ReplicateLog]follower with endpoint {} is not ready", endpoint);
                continue;
            }
            SlothStub slothStub = slothStubPool.getByEndpoint(endpoint.toString());
            // replicate log
            if (leaderStatus.getLastLogIndex() > status.getLastLogIndex()) {
                final long indexFrom = status.getLastLogIndex() + 1;
                final long indexTo = leaderStatus.getLastLogIndex();
                try {
                    List<Entry> entries = binlogger.batchGet(indexFrom, indexTo);
                    AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
                    for (Entry entry : entries) {
                        builder.addEntries(entry);
                    }
                    builder.setTerm(currentTerm);
                    builder.setLeaderIdx(leaderIdx);
                    builder.setPreLogIndex(status.getLastLogIndex());
                    builder.setPreLogTerm(status.getLastLogTerm());
                    builder.setLeaderCommitIdx(dataStore.getCommitIdx());
                    final AppendEntriesRequest request = builder.build();
                    StreamObserver<AppendEntriesResponse> observer = new StreamObserver<AppendEntriesResponse>() {
                        @Override
                        public void onNext(AppendEntriesResponse value) {
                            if (value.getSuccess()) {
                                // lock is held by main thread
                                status.setCommitIndex(request.getLeaderCommitIdx());
                                status.setLastLogTerm(currentTerm);
                                status.setLastLogIndex(indexTo);
                                status.incr();
                                logger.info("[ReplicateLog] replicate log  {} from {} to {} ", endpoint, indexFrom,
                                        indexTo);
                                finishLatch.countDown();
                            }

                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.error("fail to get append entry {} ", endpoint, t);
                        }

                        @Override
                        public void onCompleted() {

                        }
                    };
                    slothStub.getStub().appendEntries(request, observer);
                } catch (InvalidProtocolBufferException e) {
                    logger.error("fail batch get from binlogger from {} to {}", indexFrom, indexTo, e);
                }
            } else {
                finishLatch.countDown();
            }
        }
        while (finishLatch.getCount() > 0) {
            try {
                finishLatch.await(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.error("fail to wait count latch");
            }
        }
        it = batchTask.entrySet().iterator();
        while (it.hasNext()) {
            it.next().getValue().endSyncLog();
        }
        logger.info("[ReplicateLog] move log index to {} with term {}", preLogIndex, preLogTerm);
        scheduleCommit(preLogIndex, preLogTerm, commitIdx, batchTask);
    }

    /**
     * commit to local datastore
     */
    private void scheduleCommit(long preLogIndex, long preLogTerm, long commitIdx,
                                TreeMap<Long, WriteTask> entries) {
        assert mutex.isHeldByCurrentThread();
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
            HostAndPort leaderEndpoint = options.getEndpoints().get(options.getIdx());
            ReplicateLogStatus leaderStatus = logStatus.get(leaderEndpoint);
            leaderStatus.setCommitIndex(commitTo);
            writeCond.notify();
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
            writeCond.notify();
        }

    }


    private long genElectionTimeout() {
        long range = options.getMaxElectionTimeout() - options.getMinElectionTimeout();
        return options.getMinElectionTimeout() + (long) (range * random.nextDouble());
    }


    private void handleElectionTimeout() {
        try {
            mutex.lock();
            currentTerm++;
            voteCount = 1;
            logger.info("election timeout with new term {} ", currentTerm);
            role = SlothNodeRole.kCandidate;
            RequestVoteRequest request = RequestVoteRequest.newBuilder().setTerm(currentTerm).setCandidateId(options.getIdx()).build();
            mutex.unlock();
            for (int i = 0; i < options.getEndpoints().size(); i++) {
                if (i == options.getIdx()) {
                    continue;
                }
                HostAndPort endpoint = options.getEndpoints().get(i);
                SlothStub slothStub = slothStubPool.getByEndpoint(endpoint.toString());
                StreamObserver<RequestVoteResponse> observer = new StreamObserver<RequestVoteResponse>() {
                    @Override
                    public void onNext(RequestVoteResponse value) {
                        handleVoteCallback(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("fail handle request vote", t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
                slothStub.getStub().requestVote(request, observer);
            }
        } catch (Throwable t) {
            logger.error("fail to process election timeout", t);
        } finally {
            mutex.unlock();
        }
    }

    private void handleVoteCallback(RequestVoteResponse value) {
        try {
            mutex.lock();
            if (role != SlothNodeRole.kCandidate) {
                logger.warn("[Vote] the role is {}", role.toString());
                return;
            }

            if (value.getVoteGranted()) {
                voteCount++;
                int major = options.getEndpoints().size() / 2 + 1;
                logger.info("[Vote] got a vote from other node , vote count {}", voteCount);
                // become leader
                // 1. change role
                // 2. stop election timeout check
                // 3. stop vote timeout check
                // 4. start broadcast append log entry
                if (voteCount >= major) {
                    becomeToLeader();
                    return;
                }
            } else {
                logger.warn("[Vote] got a rejection vote from other node , vote count {} with term {} current term {}", voteCount, value.getTerm(),
                        currentTerm);
            }
        } catch (Throwable t) {
            logger.error("[Vote] fail to handle vote call back", t);
        } finally {
            mutex.unlock();
        }
    }

    private void handleHeartBeatCallBack(final long version,
                                         final HostAndPort endpoint,
                                         final AppendEntriesRequest request,
                                         final AppendEntriesResponse value) {
        try {
            mutex.lock();
            // become follower
            // 1. clear replicate log task when I am leader
            // 2. clear vote timeout check when I am candidate
            // 3. reset election timeout check
            // 4. change role to Follower
            // 5. update term to latest
            if (value.getTerm() > currentTerm) {
                becomeToFollower(-1, value.getTerm());
                logger.info("[AppendEntry] become follower for term update to {}", currentTerm);
            } else if (value.getSuccess()) {
                final ReplicateLogStatus status = logStatus.get(endpoint);
                if (status != null
                        && status.isMatched()
                        && request.getLeaderCommitIdx() > status.getCommitIndex()
                        && version == status.getVersion()) {
                    long oldCommitIdx = status.getCommitIndex();
                    status.setCommitIndex(request.getLeaderCommitIdx());
                    status.incr();
                    logger.info("[CommitLog] move node {} commit index from {} to {}", endpoint, oldCommitIdx, request.getLeaderCommitIdx());
                }
            }
        } catch (Throwable t) {
            logger.error("fail handle append entry call back ", t);
        } finally {
            mutex.unlock();
        }
    }

    private void hearBeat() {
        assert mutex.isHeldByCurrentThread();
        if (role != SlothNodeRole.kLeader) {
            logger.warn("[AppendEntry] exit from sync log entry for role changed ");
            return;
        }
        HostAndPort leaderEndpoint = options.getEndpoints().get(options.getIdx());
        ReplicateLogStatus leaderStatus = logStatus.get(leaderEndpoint);
        for (int i = 0; i < options.getEndpoints().size(); i++) {
            final HostAndPort endpoint = options.getEndpoints().get(i);
            if (i == options.getIdx()) {
                continue;
            }
            AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
            builder.setLeaderIdx(options.getIdx()).setTerm(currentTerm);
            builder.setLeaderCommitIdx(-1);
            final ReplicateLogStatus status = logStatus.get(endpoint);
            final long version;
            if (status != null && status.isMatched() && leaderStatus.getCommitIndex() > status.getCommitIndex()) {
                builder.setLeaderCommitIdx(leaderStatus.getCommitIndex());
                builder.setPreLogTerm(status.getLastLogTerm());
                builder.setPreLogIndex(status.getLastLogIndex());
                version = status.getVersion();
            } else {
                version = -1;
            }
            SlothStub slothStub = slothStubPool.getByEndpoint(endpoint.toString());
            final AppendEntriesRequest request = builder.build();
            StreamObserver<AppendEntriesResponse> observer = new StreamObserver<AppendEntriesResponse>() {
                @Override
                public void onNext(final AppendEntriesResponse value) {
                    handleHeartBeatCallBack(version, endpoint, request, value);
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("fail to get append entry", t);
                }

                @Override
                public void onCompleted() {
                }
            };
            slothStub.getStub().appendEntries(request, observer);
        }

        heartBeatHandle = schedPool.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    mutex.lock();
                    hearBeat();
                } finally {
                    mutex.unlock();
                }
            }
        }, options.getReplicateLogInterval(), TimeUnit.MILLISECONDS);
    }

    private void resetElectionTimeout() {
        assert mutex.isHeldByCurrentThread();
        if (!electing.get()) {
            logger.warn("election is stoped");
            return;
        }
        stopElectionTimeoutCheck();
        long timeout = genElectionTimeout();
        electionTimeoutHandle = schedPool.schedule(new Runnable() {
            @Override
            public void run() {
                voteCount = 0;
                // start a vote timeout check
                resetVoteTimeout();
                // start to broadcast vote request
                handleElectionTimeout();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private void resetVoteTimeout() {
        assert mutex.isHeldByCurrentThread();
        if (!electing.get()) {
            logger.warn("election is stoped");
            return;
        }
        stopVoteTimeoutCheck();
        requestVoteTimeoutHandle = schedPool.schedule(new Runnable() {
            @Override
            public void run() {
                resetElectionTimeout();
            }

        }, options.getVoteTimeout(), TimeUnit.MILLISECONDS);
    }

    private void resetHeartBeat() {
        assert mutex.isHeldByCurrentThread();
        if (!electing.get()) {
            logger.warn("election is stoped");
            return;
        }
        stopHeartBeat();
        hearBeat();
    }

    private void stopElectionTimeoutCheck() {
        assert mutex.isHeldByCurrentThread();
        if (electionTimeoutHandle != null) {
            electionTimeoutHandle.cancel(true);
            electionTimeoutHandle = null;
        }
    }

    private void stopVoteTimeoutCheck() {
        assert mutex.isHeldByCurrentThread();
        if (requestVoteTimeoutHandle != null) {
            requestVoteTimeoutHandle.cancel(true);
            requestVoteTimeoutHandle = null;
        }
    }

    private void stopHeartBeat() {
        assert mutex.isHeldByCurrentThread();
        if (heartBeatHandle != null) {
            heartBeatHandle.cancel(true);
            heartBeatHandle = null;
        }
    }

    public void becomeToLeader() {
        assert mutex.isHeldByCurrentThread();
        logger.info("[Vote] I am the leader with term {} with idx {} ", currentTerm, options.getIdx());
        role = SlothNodeRole.kLeader;
        resetHeartBeat();
        leaderIdx = options.getIdx();
        for (int i = 0; i < options.getEndpoints().size(); i++) {
            final HostAndPort endpoint = options.getEndpoints().get(i);
            ReplicateLogStatus status = ReplicateLogStatus.newStatus(endpoint);

            logStatus.put(endpoint, status);
            if (i == options.getIdx()) {
                status.setLastLogTerm(binlogger.getPreLogTerm());
                status.setLastLogIndex(binlogger.getPreLogIndex());
                status.setCommitIndex(dataStore.getCommitIdx());
                status.setBecomeLeaderTime(System.currentTimeMillis());
                status.setRole(SlothNodeRole.kLeader);
            } else {
                status.setBecomeFollowerTime(System.currentTimeMillis());
                status.setRole(SlothNodeRole.kFollower);
                directPool.execute(new Runnable() {
                    public void run() {
                        matchLogIndexTask(endpoint, binlogger.getPreLogIndex(), binlogger.getPreLogTerm(),
                                currentTerm);
                    }
                });
            }
        }
        stopElectionTimeoutCheck();
        stopVoteTimeoutCheck();

    }

    public void becomeToFollower(int leaderIdx, long newTerm) {
        assert mutex.isHeldByCurrentThread();
        logger.info("I am a follower with leader idx {} and term {}", leaderIdx, newTerm);
        this.leaderIdx = leaderIdx;
        role = SlothNodeRole.kFollower;
        HostAndPort endpoint = options.getEndpoints().get(options.getIdx());
        ReplicateLogStatus status = ReplicateLogStatus.newStatus(endpoint);
        status.setLastLogTerm(binlogger.getPreLogTerm());
        status.setLastLogIndex(binlogger.getPreLogIndex());
        status.setCommitIndex(dataStore.getCommitIdx());
        status.setLastApplied(dataStore.getCommitIdx());
        status.setRole(SlothNodeRole.kFollower);
        status.setBecomeFollowerTime(System.currentTimeMillis());
        logStatus.put(endpoint, status);
        stopHeartBeat();
        stopVoteTimeoutCheck();
        resetElectionTimeout();
        currentTerm = newTerm;
    }

    private void matchLogIndexTask(final HostAndPort follower,
                                   final long preLogIndex, final long preLogTerm,
                                   final long myTerm) {
        mutex.lock();
        try {
            if (role != SlothNodeRole.kLeader
                    || myTerm != currentTerm) {
                return;
            }
            ReplicateLogStatus status = logStatus.get(follower);
            if (status.isMatched()) {
                return;
            }
            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                    .setLeaderIdx(options.getIdx())
                    .setTerm(currentTerm)
                    .setLeaderCommitIdx(dataStore.getCommitIdx())
                    .setPreLogIndex(preLogIndex)
                    .setPreLogTerm(preLogTerm)
                    .build();
            SlothStub stub = slothStubPool.getByEndpoint(follower.toString());
            StreamObserver<AppendEntriesResponse> observer = new StreamObserver<AppendEntriesResponse>() {
                @Override
                public void onNext(AppendEntriesResponse value) {
                    if (value.getSuccess()) {
                        handleMatchedIndexOnLeader(follower, preLogIndex, preLogTerm);
                        return;
                    }
                    try {
                        Entry entry = binlogger.get(preLogIndex - 1);
                        matchLogIndexTask(follower, entry.getLogIndex(), entry.getTerm(), myTerm);
                    } catch (Exception e) {
                        logger.error("fail to find entry with log index {}", preLogIndex - 1);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("fail to get append entry", t);
                    schedPool.schedule(new Runnable() {
                        @Override
                        public void run() {
                            matchLogIndexTask(follower, preLogIndex, preLogTerm, myTerm);
                        }
                    }, 100, TimeUnit.MILLISECONDS);
                }

                @Override
                public void onCompleted() {
                }
            };
            stub.getStub().appendEntries(request, observer);
        } finally {
            mutex.unlock();
        }
    }

    private void handleMatchedIndexOnLeader(HostAndPort endpoint, long preLogIndex, long preLogTerm) {
        mutex.lock();
        try {
            if (role != SlothNodeRole.kLeader) {
                return;
            }
            logger.info("[LogMatch] follower {} matches leader's log with log index {} and log term {}",
                    endpoint, preLogIndex, preLogTerm);
            ReplicateLogStatus status = logStatus.get(endpoint);
            status.setMatched(true);
            status.setLastLogIndex(preLogIndex);
            status.setLastLogTerm(preLogTerm);
            status.setLastApplied(preLogIndex);
            status.setCommitIndex(preLogIndex);
        } finally {
            mutex.unlock();
        }
    }

    private boolean handleMatchIndexOnFollower(long preLogIndex, long preLogTerm) {
        mutex.lock();
        try {
            if (role != SlothNodeRole.kFollower) {
                return false;
            }
            HostAndPort endpoint = options.getEndpoints().get(options.getIdx());
            ReplicateLogStatus status = logStatus.get(endpoint);
            // log mismatch
            if (preLogIndex > status.getLastLogIndex()) {
                logger.debug("[LogMatch] log {} from leader is mismatch with my log {}",
                        preLogIndex, status.getLastLogIndex());
                return false;
            }
            // check the latest log in follower
            if (status.getLastLogIndex() == preLogIndex
                    && status.getLastLogTerm() == preLogTerm) {
                logger.debug("[LogMatch] log is matched with leader index {} term {}",
                        preLogIndex, preLogTerm);
                return true;
            }
            if (preLogIndex < status.getLastLogIndex()) {
                if (preLogIndex <= 0) {
                    logger.info("[LogMatch] clean binlogger from index 1");
                    binlogger.removeLog(1);
                    return true;
                } else {
                    Entry entry = binlogger.get(preLogIndex);
                    if (entry.getTerm() == preLogTerm) {
                        logger.info("[LogMatch] clean binlogger from index {}", preLogIndex + 1);
                        binlogger.removeLog(preLogIndex + 1);
                        return true;
                    }
                }
                return false;
            }
            return false;
        } catch (Exception e) {
            logger.error("fail to find entry with index {} ", preLogIndex, e);
            return false;
        } finally {
            mutex.unlock();
        }
    }

    public void lock() {
        mutex.lock();
    }

    public void unlock() {
        mutex.unlock();
    }

    class RaftThreadFactory implements ThreadFactory {
        private AtomicInteger counter = new AtomicInteger(0);
        private String prefix;

        public RaftThreadFactory(String prefix) {
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
