package io.microstack.sloth.monitor;

import com.alibaba.fastjson.JSON;
import com.google.common.net.HostAndPort;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.microstack.sloth.RaftCore;
import io.microstack.sloth.ReplicateLogStatus;
import io.microstack.sloth.SlothOptions;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imotai on 16/9/19.
 */
@Service
public class HttpUi extends AbstractHandler {

    @Autowired
    private RaftCore core;

    @Autowired
    private SlothOptions options;

    @Override
    public void handle(String s,
                       Request request,
                       HttpServletRequest httpServletRequest,
                       HttpServletResponse httpServletResponse) throws IOException, ServletException {
        String path = request.getPathInfo();
        if (path != null && path.equals("/cluster")) {
            handleCluster(httpServletRequest, httpServletResponse);
        }else {
            httpServletResponse.setContentType("application/json;charset=UTF-8");
            httpServletResponse.getWriter().print("hello sloth!");
        }
        request.setHandled(true);
    }

    private void handleCluster(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
        Map<String, Object> data = new HashMap<>();
        List<NodeView> views = new ArrayList<NodeView>();
        for (HostAndPort endpoint : options.getEndpoints()) {
            ReplicateLogStatus status = core.getNodeStatus(endpoint);
            if (status == null) {
                continue;
            }
            views.add(new NodeView(status));
        }
        data.put("cluster", data);
        data.put("name", "sloth");
        httpServletResponse.setContentType("application/json;charset=UTF-8");
        String jsonp = httpServletRequest.getParameter("jsonp");
        if (jsonp!=null && !jsonp.isEmpty()) {
            httpServletResponse.getWriter().print(jsonp + "(" + JSON.toJSONString(data) + ")");
        }else {
            httpServletResponse.getWriter().print(JSON.toJSONString(data));
        }
    }
}
