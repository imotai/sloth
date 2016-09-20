package io.microstack.sloth.monitor;

import io.microstack.sloth.RaftCore;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by imotai on 16/9/19.
 */
@Service
public class HttpUi extends AbstractHandler {

    @Autowired
    private RaftCore core;

    @Override
    public void handle(String s,
                       Request request,
                       HttpServletRequest httpServletRequest,
                       HttpServletResponse httpServletResponse) throws IOException, ServletException {

        httpServletResponse.setContentType("application/json;charset=UTF-8");
        httpServletResponse.getWriter().print("{hello world}");
        request.setHandled(true);
    }

    private void handleCluster(HttpServletResponse httpServletResponse) {

    }
}
