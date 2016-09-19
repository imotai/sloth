import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import io.microstack.sloth.impl.SlothNodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.net.URL;

public class Bootstrap0 {
    private final static Logger logger = LoggerFactory.getLogger(Bootstrap0.class);
    public static void main(String[] args) throws IOException, InterruptedException {
        init();
        {
            ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:sloth0.xml");
            SlothNodeImpl sloth = ctx.getBean(SlothNodeImpl.class);
            sloth.start();
            ctx.registerShutdownHook();
            logger.info("boot sloth successfully");
        }
        

    }
    
    
    private static void init() {
        try {
            URL url = Bootstrap0.class.getResource("/logback0.xml");
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(url);
        } catch (Exception ex) {
            logger.error("#Babysitter init# error e=({})", ex);
        }
    }


}
