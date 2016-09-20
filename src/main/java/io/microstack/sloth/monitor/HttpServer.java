package io.microstack.sloth.monitor;

import io.microstack.sloth.SlothOptions;
import org.eclipse.jetty.server.Server;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Created by imotai on 16/9/19.
 */
@Service
public class HttpServer {
    @Autowired
    private SlothOptions options;
    @Autowired
    private HttpUi ui;
    private Server server;

    @PostConstruct
    public void boot() throws Exception {
        server = new Server(options.getHttpPort());
        server.setHandler(ui);
        server.start();
    }

    @PreDestroy
    public void close() throws Exception {
        server.stop();
    }

}
