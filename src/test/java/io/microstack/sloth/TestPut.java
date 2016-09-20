package io.microstack.sloth;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.Test;

/**
 * Created by imotai on 16/9/19.
 */
public class TestPut {
    @Test
    public void testPut() {
        ManagedChannel channel = ManagedChannelBuilder.forTarget("127.0.0.1:9527").usePlaintext(true).build();
        SlothNodeGrpc.SlothNodeBlockingStub stub = SlothNodeGrpc.newBlockingStub(channel);

        PutRequest request = PutRequest.newBuilder().setKey("test").setValue(ByteString.copyFromUtf8("xixi")).build();
        PutResponse response = stub.put(request);
        long consume = System.currentTimeMillis();
        response = stub.put(request);
        System.out.println(System.currentTimeMillis() - consume);
        System.out.println(response.getStatus());
    }
}
