package io.microstack.sloth;

import com.google.common.net.HostAndPort;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class RaftCore {

    private final static Logger logger = LoggerFactory.getLogger(RaftCore.class);
    private final static Logger event = LoggerFactory.getLogger("event");
    private final static Logger status = LoggerFactory.getLogger("status");
    private final ReentrantLock mutex = new ReentrantLock();
    @Autowired
    private SlothOptions options;
    private int leaderIdx;
    private long currentTerm;
    private SlothNodeRole role;
    private boolean running;
    private Map<String, SlothStub> stubs = new TreeMap<String, SlothStub>();
    private Map<HostAndPort, ReplicateLogStatus> logStatus = new HashMap<HostAndPort, ReplicateLogStatus>();
    // executor for checking timeout
    private ScheduledExecutorService schedPool = Executors.newScheduledThreadPool(2, new RaftThreadFactory("schedpool"));
    private Executor directPool = Executors.newFixedThreadPool(4, new RaftThreadFactory("directpool"));
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

    public void appendLogEntries(AppendEntriesRequest request,
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
            if (request.getTerm() > currentTerm) {
                SlothNodeRole oldRole = role;
                leaderIdx = (int) request.getLeaderIdx();
                becomeToFollower(leaderIdx, request.getTerm());
                success = true;
                logger.info("[AppendEntry] become follower for term update to {}", currentTerm);
                event.info("change role from {} to {} with term {}", oldRole, role, currentTerm);
            }

            if (request.getTerm() == currentTerm
                    && role == SlothNodeRole.kFollower) {
                success = true;
            }

            if (role == SlothNodeRole.kFollower) {
                resetElectionTimeout();
            }
            success = handleMatchIndexOnFollower(request.getPreLogIndex(), request.getPreLogTerm());
            AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm).setSuccess(success).setStatus(RpcStatus.kRpcOk).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } finally {
            mutex.unlock();
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
            logger.info("election timeout with new term {}", currentTerm);
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

    private void handleHeartBeatCallBack(final AppendEntriesResponse value) {
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
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setLeaderIdx(options.getIdx()).setTerm(currentTerm).build();
        mutex.unlock();
        Iterator<String> it = stubs.keySet().iterator();
        while (it.hasNext()) {
            SlothStub slothStub = slothStubPool.getByEndpoint(it.next());
            StreamObserver<AppendEntriesResponse> observer = new StreamObserver<AppendEntriesResponse>() {
                @Override
                public void onNext(AppendEntriesResponse value) {
                    handleHeartBeatCallBack(value);
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("fail to get append entry", t);
                }

                @Override
                public void onCompleted() {}
            };
            slothStub.getStub().appendEntries(request, observer);
        }
        mutex.lock();

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
        logger.info("election time out {}", timeout);
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
            electionTimeoutHandle.cancel(false);
            electionTimeoutHandle = null;
        }
    }

    private void stopVoteTimeoutCheck() {
        assert mutex.isHeldByCurrentThread();
        if (requestVoteTimeoutHandle != null) {
            requestVoteTimeoutHandle.cancel(false);
            requestVoteTimeoutHandle = null;
        }
    }

    private void stopHeartBeat() {
        assert mutex.isHeldByCurrentThread();
        if (heartBeatHandle != null) {
            heartBeatHandle.cancel(false);
            heartBeatHandle = null;
        }
    }

    public void becomeToLeader() {
        assert mutex.isHeldByCurrentThread();
        logger.info("[Vote] I am the leader with term {} with idx {} ", currentTerm, options.getIdx());
        role = SlothNodeRole.kLeader;
        leaderIdx = options.getIdx();
        HostAndPort leaderEndpoint = options.getEndpoints().get(options.getIdx());
        ReplicateLogStatus leaderStatus = null;
        for (int i = 0; i < options.getEndpoints().size(); i++) {
            HostAndPort endpoint = options.getEndpoints().get(i);
            ReplicateLogStatus status = ReplicateLogStatus.newStatus(endpoint);
            logStatus.put(endpoint, status);
            if (i == options.getIdx()) {
                status.setLastLogTerm(binlogger.getPreLogTerm());
                status.setLastLogIndex(binlogger.getPreLogIndex());
                status.setCommitIndex(dataStore.getCommitIdx());
            }
        }
        stopElectionTimeoutCheck();
        stopVoteTimeoutCheck();
        resetHeartBeat();
    }

    public void becomeToFollower(int leaderIdx, long newTerm) {
        assert mutex.isHeldByCurrentThread();
        logger.info("I am a follower with leader idx {}", leaderIdx);
        this.leaderIdx = leaderIdx;
        role = SlothNodeRole.kFollower;
        HostAndPort endpoint = options.getEndpoints().get(options.getIdx());
        ReplicateLogStatus status = ReplicateLogStatus.newStatus(endpoint);
        status.setLastLogTerm(binlogger.getPreLogTerm());
        status.setLastLogIndex(binlogger.getPreLogIndex());
        status.setCommitIndex(dataStore.getCommitIdx());
        logStatus.put(endpoint, status);
        stopHeartBeat();
        stopVoteTimeoutCheck();
        resetElectionTimeout();
        currentTerm = newTerm;
    }

    private void matchLogIndexTask(final HostAndPort follower,
                                     final long preLogIndex, final long preLogTerm) {
        mutex.lock();
        try {
            ReplicateLogStatus status = logStatus.get(follower);
            if (status.isMatched()) {
                return;
            }

            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                                                 .setLeaderIdx(options.getIdx())
                    .setLeaderCommitIdx(dataStore.getCommitIdx()).setPreLogIndex(preLogIndex).setPreLogTerm(preLogTerm)
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
                        matchLogIndexTask(follower, entry.getLogIndex(), entry.getTerm());
                    } catch (Exception e) {
                        logger.error("fail to find entry with log index {}", preLogIndex - 1);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("fail to get append entry", t);
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
                logger.info("[LogMatch] log {} from leader is mismatch with my log {}",
                        preLogIndex, status.getLastLogIndex());
                return false;
            }
            // check the latest log in follower
            if (status.getLastLogIndex() == preLogIndex
                    && status.getLastLogTerm() == preLogTerm) {
                logger.info("[LogMatch] log is matched with leader index {} term {}",
                        preLogIndex, preLogTerm);
                return true;
            }
            if (preLogIndex < status.getLastLogIndex()) {
                Entry entry = binlogger.get(preLogIndex);
                if (entry.getTerm() == preLogTerm) {
                    binlogger.removeLog(preLogIndex + 1);
                    return true;
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
