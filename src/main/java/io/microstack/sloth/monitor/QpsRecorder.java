package io.microstack.sloth.monitor;

import com.google.protobuf.InvalidProtocolBufferException;
import io.microstack.sloth.QpsCounterLog;
import io.microstack.sloth.common.SlothThreadFactory;
import io.microstack.sloth.core.SlothOptions;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by imotai on 2016/9/25.
 */
@Service
public class QpsRecorder {
    private static final Logger logger = LoggerFactory.getLogger(QpsRecorder.class);
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new SlothThreadFactory("qps-counter"));
    static {
        try {
            RocksDB.loadLibrary();
        } catch (Throwable t) {
            logger.error("fail to load rocks db library", t);
        }
    }
    @Autowired
    private SlothOptions options;
    private RocksDB db;
    private AtomicBoolean running = new AtomicBoolean(true);

    public class Counter {
        public AtomicLong counter = new AtomicLong(0);
        public Long lastCounter = null;
        public Double qps = null;
        public Long calcInterval;
    }

    private ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<String, Counter>();

    @PostConstruct
    public void init() throws RocksDBException {
        Options dbOptions = new Options().setCreateIfMissing(true);
        db = RocksDB.open(dbOptions, options.getQpsLogPath());
        logger.info("create qps log db {} successfully", options.getQpsLogPath());
    }

    public Counter register(final String name) {
        if (counters.contains(name)) {
            return counters.get(name);
        }
        Counter c = new Counter();
        // deflaut 10ms
        c.calcInterval = 10000l;
        counters.putIfAbsent(name, c);
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                record(name);
            }
        }, c.calcInterval, TimeUnit.MILLISECONDS);
        logger.info("add counter for name {}", name);
        return c;
    }

    private void record(final String key) {
        if (!running.get()) {
            logger.info("exit record for key {}", key);
            return;
        }
        if (!counters.containsKey(key)) {
            logger.info("exit record for key {}", key);
            return;
        }
        Counter counter = counters.get(key);
        if (counter.lastCounter == null) {
            counter.lastCounter = new Long(counter.counter.get());
            logger.info("first to record qps for key {}", key);
        }else {
            long current = counter.counter.get();
            long delta = current - counter.lastCounter;
            counter.qps = delta / ((double)counter.calcInterval/1000);
            counter.lastCounter = current;
            String dbkey = key + "/" + System.currentTimeMillis();
            QpsCounterLog.Builder builder = QpsCounterLog.newBuilder();
            builder.setCtime(System.currentTimeMillis());
            builder.setKey(key);
            builder.setQps(counter.qps);
            try {
                logger.info("{} qps {} ", key, counter.qps);
                db.put(dbkey.getBytes(StandardCharsets.UTF_8), builder.build().toByteArray());
            } catch (RocksDBException e) {
                logger.error("fail to save counter log for key {}", dbkey, e);
            }
        }
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                record(key);
            }
        }, counter.calcInterval, TimeUnit.MILLISECONDS);
    }

    public List<QpsCounterLog> getRange(String key, long timeFrom, long timeTo) {
        List<QpsCounterLog> logs = new ArrayList<QpsCounterLog>();
        String startKey = key + "/" + timeFrom;
        String endKey = key + "/" + timeTo;
        RocksIterator it = db.newIterator();
        it.seek(startKey.getBytes(StandardCharsets.UTF_8));
        while (it.isValid()) {
            String tmpKey = new String(it.key());
            if (tmpKey.compareTo(endKey) > 0) {
                break;
            }
            try {
                logs.add(QpsCounterLog.parseFrom(it.value()));
                it.next();
            } catch (InvalidProtocolBufferException e) {
                logger.error("fail to parse from byte", e);
            }
        }
        return logs;
    }

    @PreDestroy
    public void close() {
        FlushOptions fopt = new FlushOptions();
        fopt.setWaitForFlush(true);
        try {
            executor.shutdown();
            db.flush(fopt);
        } catch (RocksDBException e) {
            logger.error("fail to flush db", e);
        }
        db.close();
    }
}
