package io.microstack.sloth.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.microstack.sloth.SlothNodeGrpc;
import io.microstack.sloth.common.SlothThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


@Service
public class RpcSlothStubPoolImpl implements SlothStubPool {
    private final static Logger logger = LoggerFactory.getLogger(RpcSlothStubPoolImpl.class);
    private Map<String, SlothStub> stubs = new TreeMap<String, SlothStub>();
    private Executor callbackPool;


    @PostConstruct
    public void init() {
        callbackPool = Executors.newFixedThreadPool(10, new SlothThreadFactory("rpc-pool"));
    }

    @Override
    public synchronized SlothStub getByEndpoint(String endpoint) {
        if (stubs.containsKey(endpoint)) {
            return stubs.get(endpoint);
        }
        logger.info("create stub with endpoint {}", endpoint);
        ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint)
                .executor(callbackPool).usePlaintext(true).build();
        SlothStub stub = new SlothStub();
        stub.setChannel(channel);
        stub.setFstub(SlothNodeGrpc.newFutureStub(channel));
        stub.setStub(SlothNodeGrpc.newStub(channel));
        stubs.put(endpoint, stub);
        return stub;
    }

    public Executor getCallbackPool() {
        return callbackPool;
    }

    public void setCallbackPool(Executor callbackPool) {
        this.callbackPool = callbackPool;
    }


}
