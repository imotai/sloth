package io.microstack.sloth;

import com.google.common.primitives.Longs;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by imotai on 16/9/18.
 */
@Service
public class DataStore {
    private final static Logger logger = LoggerFactory.getLogger(DataStore.class);
    private final static String COMMIT_KEY = "/commit/key";

    static {
        RocksDB.loadLibrary();
    }

    WriteOptions woptions = new WriteOptions();
    @Autowired
    private SlothOptions options;
    private AtomicLong commitIdx = new AtomicLong(0);
    private RocksDB db;

    @PostConstruct
    public void init() throws RocksDBException {
        Options dbOptions = new Options().setCreateIfMissing(true);
        db = RocksDB.open(dbOptions, options.getDataPath());
        byte[] value = db.get(COMMIT_KEY.getBytes());
        if (value == null) {
            logger.warn("no commit index exists in db, use default 0");
            commitIdx.set(0);
        } else {
            long index = Longs.fromByteArray(value);
            commitIdx.set(index);
        }
        logger.info("init data store successfully with path {} and commit idx {}", options.getDataPath(),
                commitIdx.get());
    }

    public void put(byte[] userKey, byte[] value, long logIdx) throws RocksDBException {
        WriteBatch batch = new WriteBatch();
        commitIdx.set(logIdx);
        batch.put(COMMIT_KEY.getBytes(), Longs.toByteArray(logIdx));
        batch.put(userKey, value);
        db.write(woptions, batch);
    }

    public void batchWrite(TreeMap<Long, Entry> enties) throws RocksDBException {
        if (enties.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<Long, Entry>> it = enties.entrySet().iterator();
        WriteBatch batch = new WriteBatch();
        Long commitIdxLocal = commitIdx.get();
        while (it.hasNext()) {
            Map.Entry<Long, Entry> entry = it.next();
            commitIdxLocal = entry.getValue().getLogIndex();
            batch.put(entry.getValue().getKey().getBytes(), entry.getValue().getValue().toByteArray());
        }
        batch.put(COMMIT_KEY.getBytes(), Longs.toByteArray(commitIdxLocal));
        commitIdx.set(commitIdxLocal);
        db.write(woptions, batch);
    }

    public long getCommitIdx() {
        return commitIdx.get();
    }

    @PreDestroy
    public void close() {
        db.close();
    }
}
