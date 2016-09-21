package io.microstack.sloth;

import io.grpc.ManagedChannel;

public class SlothStub {

    private SlothNodeGrpc.SlothNodeStub stub;
    private SlothNodeGrpc.SlothNodeFutureStub fstub;
    private ManagedChannel channel;

    public ManagedChannel getChannel() {
        return channel;
    }

    public void setChannel(ManagedChannel channel) {
        this.channel = channel;
    }

    public SlothNodeGrpc.SlothNodeStub getStub() {
        return stub;
    }

    public void setStub(SlothNodeGrpc.SlothNodeStub stub) {
        this.stub = stub;
    }

    public SlothNodeGrpc.SlothNodeFutureStub getFstub() {
        return fstub;
    }

    public void setFstub(SlothNodeGrpc.SlothNodeFutureStub fstub) {
        this.fstub = fstub;
    }
}
