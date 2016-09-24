package io.microstack.sloth.monitor;

import com.alibaba.fastjson.JSON;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.microstack.sloth.*;
import io.microstack.sloth.core.RaftCore;
import io.microstack.sloth.core.ReplicateLogStatus;
import io.microstack.sloth.core.SlothCore;
import io.microstack.sloth.core.SlothOptions;
import io.microstack.sloth.rpc.SlothStub;
import io.microstack.sloth.rpc.SlothStubPool;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by imotai on 16/9/19.
 */
@Service
public class HttpUi extends AbstractHandler {

    @Autowired
    private SlothCore core;

    @Autowired
    private SlothOptions options;
    @Autowired
    private SlothStubPool slothStubPool;

    @Override
    public void handle(String s,
                       Request request,
                       HttpServletRequest httpServletRequest,
                       HttpServletResponse httpServletResponse) throws IOException, ServletException {
        String path = request.getPathInfo();
        if (path != null && path.equals("/cluster")) {
            handleCluster(httpServletRequest, httpServletResponse);
        }else if ( path != null && path.equals("/put")) {
            handlePut(httpServletRequest, httpServletResponse);
        }else {
            httpServletResponse.setContentType("application/json;charset=UTF-8");
            httpServletResponse.getWriter().print("hello sloth!");
        }
        request.setHandled(true);
    }

    private void handleCluster(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
        Map<String, Object> data = new HashMap<>();
        List<NodeView> views = new ArrayList<NodeView>();
        core.getContext().getMutex().lock();
        try {
            Iterator<Map.Entry<HostAndPort, ReplicateLogStatus>> it = core.getContext().getLogStatus().entrySet().iterator();
            while (it.hasNext()) {
                ReplicateLogStatus status = it.next().getValue();
                if (status == null) {
                    continue;
                }
                views.add(new NodeView(status));
            }
        } finally {
            core.getContext().getMutex().unlock();
        }
        data.put("cluster", views);
        data.put("name", "sloth");
        data.put("leader", core.getContext().getLeaderIdx());
        httpServletResponse.setContentType("application/json;charset=UTF-8");
        String jsonp = httpServletRequest.getParameter("jsonp");
        if (jsonp!=null && !jsonp.isEmpty()) {
            httpServletResponse.getWriter().print(jsonp + "(" + JSON.toJSONString(data) + ")");
        }else {
            httpServletResponse.getWriter().print(JSON.toJSONString(data));
        }
    }

    private void handlePut(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse) throws IOException {
        String key = httpServletRequest.getParameter("key");
        String value = httpServletRequest.getParameter("value");
        int leaderIdx = core.getContext().getLeaderIdx();
        final Map<String, Object> data = new HashMap<String, Object>();
        final CountDownLatch count = new CountDownLatch(1);
        httpServletResponse.setContentType("application/json;charset=UTF-8");
        if (leaderIdx >=0) {
            SlothStub stub = slothStubPool.getByEndpoint(options.getEndpoints().get(leaderIdx).toString());
            PutRequest request = PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFromUtf8(value)).build();
            StreamObserver<PutResponse> observer = new StreamObserver<PutResponse>(){
                @Override
                public void onNext(PutResponse value) {
                    if (value.getStatus() == RpcStatus.kRpcOk) {
                        data.put("msg", "ok");
                    }else {
                        data.put("msg", "error");
                    }
                    try {
                        httpServletResponse.getWriter().print(JSON.toJSONString(data));
                    } catch (Exception e) {

                    }

                    count.countDown();
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {}
            };
            stub.getStub().put(request, observer);
        }

        try {
            count.await();
        } catch (InterruptedException e) {}
    }
}
