package io.microstack.sloth;

import com.google.common.net.HostAndPort;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicateLogStatus {

    private HostAndPort endpoint;
    private long lastLogIndex;
    private long lastLogTerm;
    private long commitIndex;
    private long lastApplied;
    // state for 
    private Map<HostAndPort, Long> nextIndex = new HashMap<HostAndPort, Long>();
    private Map<HostAndPort, Long> matchIndex = new HashMap<HostAndPort, Long>();
    private boolean matched = false;
    private Long version = 0l;
    private SlothNodeRole role;
    private long becomeLeaderTime;
    private long becomeFollowerTime;

    public ReplicateLogStatus() {}

    public static ReplicateLogStatus newStatus(HostAndPort endpoint) {
        ReplicateLogStatus status = new ReplicateLogStatus();
        status.setEndpoint(endpoint);
        status.setCommitIndex(-1);
        status.setLastApplied(-1);
        status.setLastLogIndex(-1);
        status.setLastLogTerm(-1);
        status.getNextIndex().clear();
        status.getMatchIndex().clear();
        status.setMatched(false);
        return status;
    }

    public SlothNodeRole getRole() {
        return role;
    }

    public void setRole(SlothNodeRole role) {
        this.role = role;
    }

    public long getBecomeLeaderTime() {
        return becomeLeaderTime;
    }

    public void setBecomeLeaderTime(long becomeLeaderTime) {
        this.becomeLeaderTime = becomeLeaderTime;
    }

    public long getBecomeFollowerTime() {
        return becomeFollowerTime;
    }

    public void setBecomeFollowerTime(long becomeFollowerTime) {
        this.becomeFollowerTime = becomeFollowerTime;
    }

    public void incr() {
        version++;
    }

    public long getVersion() {
        return version;
    }

    public HostAndPort getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(HostAndPort endpoint) {
        this.endpoint = endpoint;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public Map<HostAndPort, Long> getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(Map<HostAndPort, Long> nextIndex) {
        this.nextIndex = nextIndex;
    }

    public Map<HostAndPort, Long> getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(Map<HostAndPort, Long> matchIndex) {
        this.matchIndex = matchIndex;
    }

    public void resetLeader(List<HostAndPort> endpoints, int idx) {
        setEndpoint(endpoints.get(idx));
        setCommitIndex(-1);
        setLastApplied(-1);
        setLastLogIndex(-1);
        setLastLogTerm(-1);
        getNextIndex().clear();
        getMatchIndex().clear();
        setMatched(false);
    }

    public boolean isMatched() {
        return matched;
    }

    public void setMatched(boolean matched) {
        this.matched = matched;
    }

}
