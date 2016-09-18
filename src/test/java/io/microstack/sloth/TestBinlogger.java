package io.microstack.sloth;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.net.URL;

/**
 * Created by imotai on 16/9/18.
 */
public class TestBinlogger {
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

    private ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:binlogger_test.xml");

    @Test
    public void testInit() {
        Binlogger binlogger = ctx.getBean(Binlogger.class);
        try {
            binlogger.removeLog(0);
            Assert.assertEquals(0, binlogger.getPreLogIndex());

            Entry entry = Entry.newBuilder().setKey("test").setTerm(0).setValue(ByteString.copyFromUtf8("test")).build();
            long index = binlogger.append(entry);
            Assert.assertEquals(1l, index);
            Entry entrydb = binlogger.get(index);
            Assert.assertEquals("test", entrydb.getKey());
            Assert.assertEquals(1l, binlogger.getPreLogIndex());
            binlogger.removeLog(1);
            Assert.assertEquals(0, binlogger.getPreLogIndex());
            binlogger.close();
        } catch (Exception e) {
            Assert.assertTrue(false);
        }

    }
}
