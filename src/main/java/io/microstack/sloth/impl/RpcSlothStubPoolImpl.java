package io.microstack.sloth.impl;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.microstack.sloth.SlothNodeGrpc;
import io.microstack.sloth.SlothOptions;
import io.microstack.sloth.SlothStub;
import io.microstack.sloth.SlothStubPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


@Service
public class RpcSlothStubPoolImpl implements SlothStubPool {
    private final static Logger logger = LoggerFactory.getLogger(RpcSlothStubPoolImpl.class);
    private Map<String, SlothStub> stubs = new TreeMap<String, SlothStub>();
    private Executor callbackPool ;
    @Autowired
    private SlothOptions options;

    @PostConstruct
    public void init() {
        callbackPool = Executors.newFixedThreadPool(10, new CallbackThreadFactory("callback"));
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

    class CallbackThreadFactory implements ThreadFactory {
        private AtomicInteger counter = new AtomicInteger(0);
        private String prefix;

        public CallbackThreadFactory(String prefix) {
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
