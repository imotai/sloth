package io.microstack.sloth.task;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import io.microstack.sloth.*;
import io.microstack.sloth.common.GSchedThreadPool;
import io.microstack.sloth.common.SlothThreadFactory;
import io.microstack.sloth.context.SlothContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by imotai on 16/9/21.
 */

public class TaskManager {
    private final static Logger logger = LoggerFactory.getLogger(RaftCore.class);
    private Random random;
    private SlothContext context;
    private SlothOptions options;
    private Map<TaskType, Task> tasks = new HashMap<TaskType, Task>();
    private SlothStubPool stubPool;
    public TaskManager(SlothContext context, SlothOptions options, SlothStubPool stubPool) {
        this.context = context;
        this.options = options;
        this.stubPool = stubPool;
        random = new Random(System.nanoTime());
    }

    public enum TaskType {
        kElectionTask,
        kWaitVoteTask,
        kHeartBeatTask
    }

    class Task {
        public TaskType type;
        public ScheduledFuture<?> handle;
    }


    public void init() {

    }


    private void handleElectionTimeout() {
        context.getMutex().lock();
        try {
            if (context.getRole() == SlothNodeRole.kLeader) {
                return;
            }
            // trigger election
            long oldTerm = context.getCurrentTerm();
            context.setCurrentTerm(oldTerm + 1);
            context.setRole(SlothNodeRole.kCandidate);
            ReplicateLogStatus status = context.getLogStatus().get(context.getEndpoint());
            RequestVoteRequest.Builder builder = RequestVoteRequest.newBuilder();
            builder.setCandidateId(options.getIdx());
            builder.setReqIdx(options.getIdx());
            builder.setLastLogIndex(status.getLastLogIndex());
            builder.setLastLogTerm(status.getLastLogTerm());
            RequestVoteRequest request = builder.build();
            context.getMutex().unlock();
            Map<HostAndPort, ListenableFuture<RequestVoteResponse>> futures = new HashMap<>();
            for (int i = 0; i < options.getEndpoints().size(); i++) {
                if (i == options.getIdx()) {
                    continue;
                }
                HostAndPort endpoint = options.getEndpoints().get(i);
                SlothStub slothStub = stubPool.getByEndpoint(endpoint.toString());
                SlothNodeGrpc.SlothNodeFutureStub fstub = slothStub.getFstub();
                ListenableFuture<RequestVoteResponse> fresponse = fstub.requestVote(request);
                futures.put(endpoint, fresponse);
            }
            processVoteResult(futures);
        } finally {
            if (context.getMutex().isHeldByCurrentThread()) {
                context.getMutex().unlock();
            }
        }
    }

    private void processVoteResult(Map<HostAndPort, ListenableFuture<RequestVoteResponse>> futures) {
        int voteCount = 1;
        int major = options.getEndpoints().size() / 2 + 1;
        Set<HostAndPort> votedNode = new HashSet<HostAndPort>();
        while (votedNode.size() < futures.size() && voteCount < major ) {
            Iterator<Map.Entry<HostAndPort, ListenableFuture<RequestVoteResponse>>> it = futures.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<HostAndPort, ListenableFuture<RequestVoteResponse>> entry = it.next();
                if (entry.getValue().isDone()) {
                    votedNode.add(entry.getKey());
                    try {
                        RequestVoteResponse response = entry.getValue().get();
                        if (response.getVoteGranted()) {
                            voteCount++;
                            logger.info("[Vote]node {} votes me to be leader", entry.getKey());
                        }else {
                            logger.info("[Vote]node {} rejects me to be leader", entry.getKey());
                        }
                    } catch (Exception e) {
                        logger.error("fail to get vote response from {}", entry.getKey());
                    }
                }
            }
        }
        context.getMutex().lock();
        try {
            if (context.getRole() != SlothNodeRole.kCandidate) {
                return;
            }
            if (voteCount > major) {
                becomeToLeader();
            } else {
                logger.warn("[Vote] election fails , got vote count {}", voteCount);
            }
        } finally {
            if (context.getMutex().isHeldByCurrentThread()) {
                context.getMutex().unlock();
            }
        }
    }

    private void keepLeaderAuthority() {
        context.getMutex().lock();
        try {
            if (context.getRole() != SlothNodeRole.kLeader) {
                return;
            }
            HostAndPort leaderEndpoint = options.getEndpoints().get(options.getIdx());
            ReplicateLogStatus leaderStatus = context.getLogStatus().get(leaderEndpoint);
            Map<HostAndPort, ListenableFuture<AppendEntriesResponse>> futures =
                    new HashMap<HostAndPort, ListenableFuture<AppendEntriesResponse>>();
            Map<HostAndPort, Long> reqVersion = new HashMap<HostAndPort, Long>();
            Map<HostAndPort, AppendEntriesRequest> requests = new HashMap<HostAndPort, AppendEntriesRequest>();
            for (HostAndPort endpoint : options.getEndpoints()) {
                if (endpoint.equals(leaderEndpoint)) {
                    continue;
                }
                ReplicateLogStatus status = context.getLogStatus().get(endpoint);
                AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
                builder.setReqIdx(options.getIdx());
                builder.setLeaderCommitIdx(leaderStatus.getCommitIndex());
                builder.setPreLogIndex(status.getLastLogIndex());
                builder.setPreLogTerm(status.getLastLogTerm());
                builder.setLeaderIdx(options.getIdx());
                builder.setTerm(context.getCurrentTerm());
                AppendEntriesRequest request = builder.build();
                SlothStub slothStub = stubPool.getByEndpoint(endpoint.toString());
                SlothNodeGrpc.SlothNodeFutureStub fstub = slothStub.getFstub();
                ListenableFuture<AppendEntriesResponse> fresponse = fstub.appendEntries(request);
                futures.put(endpoint, fresponse);
                reqVersion.put(endpoint, status.getVersion());
                requests.put(endpoint, request);
            }
            context.getMutex().unlock();
            Iterator<Map.Entry<HostAndPort, ListenableFuture<AppendEntriesResponse>>> it = futures.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<HostAndPort, ListenableFuture<AppendEntriesResponse>> entry = it.next();
                try {
                    entry.getValue().get();
                } catch (Exception e) {
                    logger.error("fail to append entry to {} ", entry.getKey());
                }
            }
            processKeepLeaderAuthorityResult(futures, reqVersion,requests);
            Task task = tasks.get(TaskType.kHeartBeatTask);
            task.handle = GSchedThreadPool.getInstance().schedule(new Runnable() {
                @Override
                public void run() {
                    keepLeaderAuthority();
                }
            }, options.getReplicateLogInterval());
        } finally {
            if (context.getMutex().isHeldByCurrentThread()) {
                context.getMutex().unlock();
            }
        }

    }

    private void processKeepLeaderAuthorityResult(Map<HostAndPort, ListenableFuture<AppendEntriesResponse>> futures,
                                                  Map<HostAndPort, Long> reqVersion,
                                                  Map<HostAndPort, AppendEntriesRequest> requests) {
        context.getMutex().lock();
        try {
            Iterator<Map.Entry<HostAndPort, ListenableFuture<AppendEntriesResponse>>> it = futures.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<HostAndPort, ListenableFuture<AppendEntriesResponse>> entry = it.next();
                ReplicateLogStatus status = context.getLogStatus().get(entry.getKey());
                AppendEntriesRequest request = requests.get(entry.getKey());
                long version = reqVersion.get(entry.getKey());
                if (version != status.getVersion()) {
                    logger.warn("status version mismatch, ignore append entry return from {}", entry.getKey());
                    continue;
                }

                try {
                    AppendEntriesResponse response = entry.getValue().get();
                    if (!response.getSuccess()) {
                        continue;
                    }
                    status.setMatched(true);
                    if (status.getCommitIndex() < request.getLeaderCommitIdx()) {
                        long oldCommitIdx = status.getCommitIndex();
                        status.setCommitIndex(request.getLeaderCommitIdx());
                        status.incr();
                        logger.info("[Commit] move commit index from {} to {} for node {}", oldCommitIdx,
                                status.getCommitIndex(), entry.getKey());
                    }
                } catch (Exception e) {
                    logger.error("fail to append entry to {} ", entry.getKey());
                    continue;
                }
            }
        } finally {
            if (context.getMutex().isHeldByCurrentThread()) {
                context.getMutex().unlock();
            }
        }

    }

    /**
     * 1.change role to leader and reset all follower's status
     * 2.start keepLeaderAuthority task
     * 3.stop election timeout check task
     * 4.stop vote timeout check task
     *
     **/
    private void becomeToLeader() {
        assert context.getMutex().isHeldByCurrentThread();
        context.resetToLeader();
        resetDelayTask(TaskType.kHeartBeatTask);
        stopTask(TaskType.kElectionTask);
        stopTask(TaskType.kWaitVoteTask);
    }

    public void stopTask(TaskType type) {
        assert context.getMutex().isHeldByCurrentThread();
    }

    public void resetDelayTask(TaskType type) {
        assert context.getMutex().isHeldByCurrentThread();
        if (tasks.containsKey(type)) {
            Task task = tasks.get(type);
            if (task.handle != null) {
                task.handle.cancel(false);
            }
            tasks.remove(type);
        }
        switch (type) {
            case kElectionTask:
            {
                Task task = new Task();
                task.type = type;
                long delay = genElectionTimeout();
                task.handle = GSchedThreadPool.getInstance().schedule(new Runnable() {
                    @Override
                    public void run() {
                        handleElectionTimeout();
                    }
                }, delay);
                tasks.put(type, task);
            }
            break;
            case kWaitVoteTask:
            {
                Task task = new Task();
                task.type = type;
                task.handle = GSchedThreadPool.getInstance().schedule(new Runnable() {
                    @Override
                    public void run() {
                        logger.warn("[Vote] vote timeout");
                        handleElectionTimeout();
                    }
                }, options.getVoteTimeout());
                tasks.put(type, task);
            }
            break;
            case kHeartBeatTask:
            {
                Task task = new Task();
                task.type = type;
                task.handle = null;
                tasks.put(type, task);
                // run it right now
                keepLeaderAuthority();
            }
        }

    }

    private long genElectionTimeout() {
        long range = options.getMaxElectionTimeout() - options.getMinElectionTimeout();
        return options.getMinElectionTimeout() + (long) (range * random.nextDouble());
    }

}
