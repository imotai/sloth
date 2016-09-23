package io.microstack.sloth.core;

import io.grpc.stub.StreamObserver;
import io.microstack.sloth.Entry;
import io.microstack.sloth.PutRequest;
import io.microstack.sloth.PutResponse;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

/**
 * Created by imotai on 16/9/19.
 */
public class WriteTask {
    private PutRequest request;
    private StreamObserver<PutResponse> responseObserver;
    private Condition condition;
    private boolean done = false;
    private PutResponse response;
    private long index = -1;
    private AtomicInteger count = new AtomicInteger(0);
    private Entry entry;
    private long writeLogConsumed;
    private long syncLogConsumed;
    private long commitToLocalConsumed;
    private long waitToWrite;

    public WriteTask(PutRequest request, StreamObserver<PutResponse> responseObserver, Condition condition) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.condition = condition;
    }

    public long getWaitToWrite() {
        return waitToWrite;
    }

    public void setWaitToWrite(long waitToWrite) {
        this.waitToWrite = waitToWrite;
    }

    public void startWaitToWrite() {
        waitToWrite = System.currentTimeMillis();
    }

    public void endWaitToWrite() {
        waitToWrite = System.currentTimeMillis() - waitToWrite;
    }

    public long getWriteLogConsumed() {
        return writeLogConsumed;
    }

    public void setWriteLogConsumed(long writeLogConsumed) {
        this.writeLogConsumed = writeLogConsumed;
    }

    public long getSyncLogConsumed() {
        return syncLogConsumed;
    }

    public void setSyncLogConsumed(long syncLogConsumed) {
        this.syncLogConsumed = syncLogConsumed;
    }

    public long getCommitToLocalConsumed() {
        return commitToLocalConsumed;
    }

    public void setCommitToLocalConsumed(long commitToLocalConsumed) {
        this.commitToLocalConsumed = commitToLocalConsumed;
    }

    public void startWriteLog() {
        writeLogConsumed = System.currentTimeMillis();
    }

    public void endWriteLog() {
        writeLogConsumed = System.currentTimeMillis() - writeLogConsumed;
    }

    public void startSyncLog() {
        syncLogConsumed = System.currentTimeMillis();
    }

    public void endSyncLog() {
        syncLogConsumed = System.currentTimeMillis() - syncLogConsumed;
    }

    public void startCommit() {
        commitToLocalConsumed = System.currentTimeMillis();
    }

    public void endCommit() {
        commitToLocalConsumed = System.currentTimeMillis() - commitToLocalConsumed;
    }

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public PutResponse getResponse() {
        return response;
    }

    public void setResponse(PutResponse response) {
        this.response = response;
    }

    public void incr() {
        count.incrementAndGet();
    }

    public int getCount() {
        return count.get();
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public StreamObserver<PutResponse> getResponseObserver() {
        return responseObserver;
    }

    public void setResponseObserver(StreamObserver<PutResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public PutRequest getRequest() {
        return request;
    }

    public void setRequest(PutRequest request) {
        this.request = request;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WriteTask writeTask = (WriteTask) o;
        if (request != null && request.equals(writeTask.request)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = request != null ? request.hashCode() : 0;
        result = 31 * result + (responseObserver != null ? responseObserver.hashCode() : 0);
        result = 31 * result + (condition != null ? condition.hashCode() : 0);
        result = 31 * result + (done ? 1 : 0);
        result = 31 * result + (count != null ? count.hashCode() : 0);
        return result;
    }
}
