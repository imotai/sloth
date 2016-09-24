package io.microstack.sloth.context;

import com.google.common.net.HostAndPort;
import io.microstack.sloth.core.ReplicateLogStatus;
import io.microstack.sloth.SlothNodeRole;
import io.microstack.sloth.core.SlothOptions;
import io.microstack.sloth.core.WriteTask;
import io.microstack.sloth.log.Binlogger;
import io.microstack.sloth.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by imotai on 16/9/21.
 */

public class SlothContext {
    private static final Logger status = LoggerFactory.getLogger("status");
    private static final Logger logger = LoggerFactory.getLogger(SlothContext.class);
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition writeCond = mutex.newCondition();
    private int leaderIdx;
    private long currentTerm;
    private SlothNodeRole role;
    private boolean running;
    private Map<HostAndPort, ReplicateLogStatus> logStatus = new HashMap<HostAndPort, ReplicateLogStatus>();
    private AtomicBoolean electing = new AtomicBoolean(false);
    private Map<Long, Integer> votedFor = new HashMap<Long, Integer>();
    private List<WriteTask> tasks = new LinkedList<WriteTask>();
    private HostAndPort endpoint;

    private Binlogger binlogger;
    private DataStore dataStore;
    private SlothOptions options;
    public SlothContext(Binlogger binlogger,
                        DataStore dataStore,
                        SlothOptions options) {
        this.binlogger = binlogger;
        this.dataStore = dataStore;
        this.options = options;
    }

    public HostAndPort getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(HostAndPort endpoint) {
        this.endpoint = endpoint;
    }

    public AtomicBoolean getElecting() {
        return electing;
    }

    public void setElecting(AtomicBoolean electing) {
        this.electing = electing;
    }

    public Map<Long, Integer> getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(Map<Long, Integer> votedFor) {
        this.votedFor = votedFor;
    }

    public List<WriteTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<WriteTask> tasks) {
        this.tasks = tasks;
    }

    public ReentrantLock getMutex() {
        return mutex;
    }

    public Condition getWriteCond() {
        return writeCond;
    }

    public int getLeaderIdx() {
        return leaderIdx;
    }

    public void setLeaderIdx(int leaderIdx) {
        this.leaderIdx = leaderIdx;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public SlothNodeRole getRole() {
        return role;
    }

    public void setRole(SlothNodeRole role) {
        this.role = role;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public Map<HostAndPort, ReplicateLogStatus> getLogStatus() {
        return logStatus;
    }

    public void setLogStatus(Map<HostAndPort, ReplicateLogStatus> logStatus) {
        this.logStatus = logStatus;
    }

    public void resetToFollower(int leaderIdx, long newTerm) {
        assert mutex.isHeldByCurrentThread();
        this.leaderIdx = leaderIdx;
        role = SlothNodeRole.kFollower;
        logger.info("[Vote] I am the follower with term {} with leader idx {} ", newTerm, leaderIdx);
        logStatus.clear();
        ReplicateLogStatus status = ReplicateLogStatus.newStatus(endpoint);
        status.setLastLogTerm(binlogger.getPreLogTerm());
        status.setLastLogIndex(binlogger.getPreLogIndex());
        status.setCommitIndex(dataStore.getCommitIdx());
        status.setLastApplied(dataStore.getCommitIdx());
        status.setRole(SlothNodeRole.kFollower);
        status.setBecomeFollowerTime(System.currentTimeMillis());
        logStatus.put(endpoint, status);
    }

    public void resetToLeader() {
        assert mutex.isHeldByCurrentThread();
        logger.info("[Vote] I am the leader with term {} with idx {} ", currentTerm, options.getIdx());
        logStatus.clear();
        role = SlothNodeRole.kLeader;
        leaderIdx = options.getIdx();
        for (int i = 0; i < options.getEndpoints().size(); i++) {
            final HostAndPort nodeEndpoint = options.getEndpoints().get(i);
            ReplicateLogStatus status = ReplicateLogStatus.newStatus(nodeEndpoint);
            status.setMatched(false);
            logStatus.put(nodeEndpoint, status);
            status.setLastLogTerm(binlogger.getPreLogTerm());
            status.setLastLogIndex(binlogger.getPreLogIndex());
            status.setCommitIndex(dataStore.getCommitIdx());
            status.setLastApplied(dataStore.getCommitIdx());
            if (i == options.getIdx()) {
                status.setBecomeLeaderTime(System.currentTimeMillis());
                status.setRole(SlothNodeRole.kLeader);
            } else {
                status.setBecomeFollowerTime(System.currentTimeMillis());
                status.setRole(SlothNodeRole.kFollower);
            }
        }
    }

    public boolean isLeader() {
        assert mutex.isHeldByCurrentThread();
        return role == SlothNodeRole.kLeader;
    }

    public boolean isFollower() {
        assert mutex.isHeldByCurrentThread();
        return role == SlothNodeRole.kFollower;
    }

    public boolean isCandidate() {
        assert mutex.isHeldByCurrentThread();
        return role == SlothNodeRole.kCandidate;
    }
}
