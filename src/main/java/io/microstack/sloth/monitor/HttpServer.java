package io.microstack.sloth.monitor;

import io.microstack.sloth.core.SlothOptions;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
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
        ResourceHandler rhandle = new ResourceHandler();
        rhandle.setDirectoriesListed(true);
        rhandle.setWelcomeFiles(new String[]{ "index.html" });
        rhandle.setResourceBase(options.getResourcePath());
        HandlerList handleList = new HandlerList();
        handleList.setHandlers(new Handler[]{rhandle, ui});
        server.setHandler(handleList);
        server.start();
    }

    @PreDestroy
    public void close() throws Exception {
        server.stop();
    }

}
