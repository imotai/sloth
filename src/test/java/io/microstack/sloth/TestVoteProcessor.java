package io.microstack.sloth;

import com.google.common.util.concurrent.ListenableFuture;
import io.microstack.sloth.rpc.RpcSlothStubPoolImpl;
import io.microstack.sloth.rpc.SlothStub;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 * Created by imotai on 2016/9/23.
 */
public class TestVoteProcessor {
   /* private ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:request_vote_test.xml");

    @Ignore
    @Test
    public void testVote() {
        SlothOptions options = ctx.getBean(SlothOptions.class);
        Binlogger binlogger = ctx.getBean(Binlogger.class);
        try {
            binlogger.removeLog(1);
            Assert.assertEquals(0, binlogger.getPreLogIndex());
            Assert.assertEquals(0, binlogger.getPreLogTerm());
        } catch (Exception e) {
            e.printStackTrace();
        }
        DataStore dataStore = ctx.getBean(DataStore.class);
        SlothContext context = new SlothContext(binlogger, dataStore, options);
        {
            context.getMutex().lock();
            context.resetToFollower(-1, 0);
            context.getMutex().unlock();
        }

        RequestVoteProcessor processor = new RequestVoteProcessor(context, options);
        RequestVoteRequest.Builder builder = RequestVoteRequest.newBuilder();
        builder.setLastLogTerm(0);
        builder.setLastLogIndex(0);
        builder.setTerm(1);
        builder.setReqIdx(4);
        builder.setCandidateId(4);
        StreamObserver<RequestVoteResponse> responseObserver = new StreamObserver<RequestVoteResponse>(){

            @Override
            public void onNext(RequestVoteResponse value) {
                Assert.assertTrue(value.getVoteGranted());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
        processor.process(builder.build(), responseObserver);
    }

*/
    @Test
    public void test2() {
        RpcSlothStubPoolImpl pool = new RpcSlothStubPoolImpl();
        SlothStub stub = pool.getByEndpoint("127.0.0.1:9527");
        SlothNodeGrpc.SlothNodeFutureStub fstub = stub.getFstub();
        RequestVoteRequest.Builder builder = RequestVoteRequest.newBuilder();
        builder.setLastLogTerm(0);
        builder.setLastLogIndex(0);
        builder.setTerm(1000000000000000000l);
        builder.setReqIdx(4);
        builder.setCandidateId(4);
        ListenableFuture<RequestVoteResponse> freponse = fstub.requestVote(builder.build());
        try {
            freponse.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

}
