package io.microstack.sloth.monitor;

import com.google.common.net.HostAndPort;
import io.microstack.sloth.core.ReplicateLogStatus;
import io.microstack.sloth.SlothNodeRole;

/**
 * Created by imotai on 16/9/20.
 */
public class NodeView {

    private HostAndPort endpoint;
    private long lastLogIndex;
    private long lastLogTerm;
    private long commitIndex;
    private long lastApplied;

    private boolean matched = false;
    private SlothNodeRole role;
    private long becomeLeaderTime;
    private long becomeFollowerTime;

    public NodeView(ReplicateLogStatus status) {
        setRole(status.getRole());
        setEndpoint(status.getEndpoint());
        setBecomeLeaderTime(status.getBecomeLeaderTime());
        setBecomeFollowerTime(status.getBecomeFollowerTime());
        setCommitIndex(status.getCommitIndex());
        setLastLogIndex(status.getLastLogIndex());
        setLastLogTerm(status.getLastLogTerm());
        setMatched(status.isMatched());
        setLastApplied(status.getLastApplied());
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

    public boolean isMatched() {
        return matched;
    }

    public void setMatched(boolean matched) {
        this.matched = matched;
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
}
