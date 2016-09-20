package io.microstack.sloth;

import com.google.common.net.HostAndPort;

import java.util.ArrayList;
import java.util.List;

public class SlothOptions {

    // the max election timeout
    private long maxElectionTimeout;
    // the min election timeout
    private long minElectionTimeout;

    // the index of endpoints
    private int idx;
    // the cluster endpoints
    private List<HostAndPort> endpoints = new ArrayList<HostAndPort>();

    private long voteTimeout;

    private int callbackPoolSize;

    private long replicateLogInterval;

    private String endpointStr;

    private String binlogPath;

    public String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    private String resourcePath;

    private String dataPath;
    private int httpPort;

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public String getBinlogPath() {
        return binlogPath;
    }

    public void setBinlogPath(String binlogPath) {
        this.binlogPath = binlogPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getEndpointStr() {
        return endpointStr;
    }

    public void setEndpointStr(String endpointStr) {
        String[] endpointArray = endpointStr.split(",");
        for (String e : endpointArray) {
            addEndpoint(e);
        }
    }

    public long getReplicateLogInterval() {
        return replicateLogInterval;
    }

    public void setReplicateLogInterval(long replicateLogInterval) {
        this.replicateLogInterval = replicateLogInterval;
    }

    public int getCallbackPoolSize() {
        return callbackPoolSize;
    }

    public void setCallbackPoolSize(int callbackPoolSize) {
        this.callbackPoolSize = callbackPoolSize;
    }

    public long getMaxElectionTimeout() {
        return maxElectionTimeout;
    }

    public void setMaxElectionTimeout(long maxElectionTimeout) {
        this.maxElectionTimeout = maxElectionTimeout;
    }

    public long getMinElectionTimeout() {
        return minElectionTimeout;
    }

    public void setMinElectionTimeout(long minElectionTimeout) {
        this.minElectionTimeout = minElectionTimeout;
    }

    public void addEndpoint(String endpoint) {
        endpoints.add(HostAndPort.fromString(endpoint));
    }


    public List<HostAndPort> getEndpoints() {
        return endpoints;
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public long getVoteTimeout() {
        return voteTimeout;
    }

    public void setVoteTimeout(long voteTimeout) {
        this.voteTimeout = voteTimeout;
    }


}
