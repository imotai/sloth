package io.microstack.sloth.log;

import com.google.protobuf.InvalidProtocolBufferException;
import io.microstack.sloth.Entry;
import io.microstack.sloth.core.SlothOptions;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class Binlogger {

    private final static Logger logger = LoggerFactory.getLogger(Binlogger.class);
    private final static String BINLOGGER_PREFIX = "/BINLOGGER/";

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
    private AtomicLong preLogIndex = new AtomicLong(0);
    private AtomicLong preLogTerm = new AtomicLong(0);
    private Object tx = new Object();
    private WriteOptions woptions = new WriteOptions();

    @PostConstruct
    public void init() throws Exception {
        Options dbOptions = new Options().setCreateIfMissing(true);
        db = RocksDB.open(dbOptions, options.getBinlogPath());
        logger.info("create db {} successfully", options.getBinlogPath());
        initLogIndex();
    }

    private void initLogIndex() throws InvalidProtocolBufferException {
        synchronized (tx) {
            RocksIterator it = db.newIterator();
            it.seekToLast();
            if (!it.isValid()) {
                preLogIndex.set(0);
            } else {
                String key = new String(it.key());
                Entry entry = Entry.parseFrom(it.value());
                preLogTerm.set(entry.getTerm());
                preLogIndex.set(entry.getLogIndex());
            }
            logger.info("init binlogger with log index {} and term {}", preLogIndex.get(), preLogTerm.get());
        }
    }


    public void append(Entry entry) throws RocksDBException {

        synchronized (tx) {
            preLogTerm.set(entry.getTerm());
            String key = BINLOGGER_PREFIX + entry.getLogIndex();
            woptions.setSync(false);
            db.put(woptions, key.getBytes(), entry.toByteArray());
        }
    }

    public void batchWrite(List<Entry> entries) throws RocksDBException {
        synchronized (tx) {
            WriteBatch batch = new WriteBatch();
            for (Entry entry : entries) {
                String key = BINLOGGER_PREFIX + entry.getLogIndex();
                batch.put(key.getBytes(), entry.toByteArray());
                preLogIndex.set(entry.getLogIndex());
                preLogTerm.set(entry.getTerm());
            }
            db.write(woptions, batch);
        }
    }


    public List<Entry> batchGet(long from, long to) throws InvalidProtocolBufferException {
        String startKey = BINLOGGER_PREFIX + from;
        String endKey = BINLOGGER_PREFIX + to;
        RocksIterator it = db.newIterator();
        it.seek(startKey.getBytes());
        List<Entry> entries = new ArrayList<Entry>();
        while (it.isValid()) {
            String key = new String(it.key());
            if (key.compareTo(endKey) > 0) {
                break;
            }
            Entry entry = Entry.parseFrom(it.value());
            entries.add(entry);
            it.next();
        }
        return entries;
    }

    public Entry get(long index) throws Exception {
        String key = BINLOGGER_PREFIX + index;
        byte[] value = db.get(key.getBytes());
        if (value == null) {
            throw new Exception("key with index " + index + " does not exist");
        }
        Entry entry = Entry.parseFrom(value);
        return entry;
    }

    public void removeLog(long from) throws Exception {
        RocksIterator it = db.newIterator();
        String startKey = BINLOGGER_PREFIX + from;
        it.seek(startKey.getBytes());
        WriteBatch batch = new WriteBatch();
        while (it.isValid()) {
            batch.remove(it.key());
            it.next();
        }
        db.write(woptions, batch);
        initLogIndex();
    }

    public long getPreLogIndex() {
        return preLogIndex.get();
    }


    public long getPreLogTerm() {
        return preLogTerm.get();
    }

    public long getDataSize() {
        try {
            return db.getLongProperty("rocksdb.estimate-live-data-size");
        } catch (RocksDBException e) {
            logger.error("fail to get properties rocksdb.estimate-live-data-size", e);
            return 0l;
        }
    }

    @PreDestroy
    public void close() {
        synchronized (tx) {
            FlushOptions fopt = new FlushOptions();
            fopt.setWaitForFlush(true);
            try {
                db.flush(fopt);
            } catch (RocksDBException e) {
                logger.error("fail to flush db", e);
            }
            db.close();
        }
    }

}
