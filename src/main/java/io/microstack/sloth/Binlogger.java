package io.microstack.sloth;

import com.google.protobuf.InvalidProtocolBufferException;
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
        RocksIterator it = db.newIterator();
        it.seekToLast();
        if (!it.isValid()) {
            preLogIndex.set(0);
        } else {
            String key = new String(it.key());
            preLogIndex.set(Long.parseLong(key.replace(BINLOGGER_PREFIX, "")));
            Entry entry = Entry.parseFrom(it.value());
            preLogTerm.set(entry.getTerm());
        }
        logger.info("init binlogger with log index {} and term {}", preLogIndex.get(), preLogTerm.get());
    }


    public long append(Entry entry) throws RocksDBException {
        long index = 0;
        synchronized (tx) {
            index = preLogIndex.incrementAndGet();
            preLogTerm.set(entry.getTerm());
        }
        String key = BINLOGGER_PREFIX + index;
        WriteOptions wopt = new WriteOptions();
        wopt.setSync(false);
        db.put(wopt, key.getBytes(), entry.toByteArray());
        return index;
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

    @PreDestroy
    public void close() {
        db.close();
    }


}