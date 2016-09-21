package io.microstack.sloth;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import io.grpc.stub.StreamObserver;
import io.microstack.sloth.log.Binlogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.net.URL;

/**
 * Created by imotai on 16/9/18.
 */
public class TestRaftCore {
    static {
        try {
            URL url = Bootstrap.class.getResource("/logback0.xml");
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(url);
        } catch (Exception ex) {

        }

    }

    private ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:raftcore_test.xml");


    @Test
    public void testAppendLogToFollower() {
        RaftCore core = ctx.getBean(RaftCore.class);
        core.start();
        core.stopElection();
        core.lock();
        core.becomeToFollower(1, 1);
        core.unlock();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setLeaderCommitIdx(1).setTerm(1)
                .setPreLogTerm(0).setPreLogIndex(0).setLeaderIdx(1).build();
        StreamObserver<AppendEntriesResponse> responseObserver = new StreamObserver<AppendEntriesResponse>(){
            @Override
            public void onNext(AppendEntriesResponse value) {
                Assert.assertEquals(true, value.getSuccess());
                Assert.assertEquals(1, value.getTerm());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };

        core.appendLogEntries(request, responseObserver);
        Binlogger binlogger = ctx.getBean(Binlogger.class);
        try {
            binlogger.removeLog(0);
            Entry entry1 = Entry.newBuilder().setKey("test").setTerm(1).build();
            binlogger.append(entry1);
            Entry entry2 = Entry.newBuilder().setKey("test").setTerm(1).build();
            binlogger.append(entry2);
            core.lock();
            core.becomeToFollower(1, 1);
            core.unlock();
            AppendEntriesRequest request2 = AppendEntriesRequest.newBuilder().setLeaderCommitIdx(1).setTerm(1)
                    .setPreLogTerm(1).setPreLogIndex(3).setLeaderIdx(1).build();
            StreamObserver<AppendEntriesResponse> responseObserver2 = new StreamObserver<AppendEntriesResponse>(){
                @Override
                public void onNext(AppendEntriesResponse value) {
                    Assert.assertEquals(false, value.getSuccess());

                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
            core.appendLogEntries(request2, responseObserver2);
            Assert.assertEquals(2, binlogger.getPreLogIndex());
            AppendEntriesRequest request3 = AppendEntriesRequest.newBuilder().setLeaderCommitIdx(1).setTerm(1)
                    .setPreLogTerm(1).setPreLogIndex(2).setLeaderIdx(1).build();
            StreamObserver<AppendEntriesResponse> responseObserver3 = new StreamObserver<AppendEntriesResponse>(){
                @Override
                public void onNext(AppendEntriesResponse value) {
                    Assert.assertEquals(true, value.getSuccess());

                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
            core.appendLogEntries(request3, responseObserver3);

            AppendEntriesRequest request4 = AppendEntriesRequest.newBuilder().setLeaderCommitIdx(1).setTerm(1)
                    .setPreLogTerm(1).setPreLogIndex(1).setLeaderIdx(1).build();
            StreamObserver<AppendEntriesResponse> responseObserver4 = new StreamObserver<AppendEntriesResponse>(){
                @Override
                public void onNext(AppendEntriesResponse value) {
                    Assert.assertEquals(true, value.getSuccess());

                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
            core.appendLogEntries(request4, responseObserver4);
            Assert.assertEquals(1l, binlogger.getPreLogIndex());
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }
}
