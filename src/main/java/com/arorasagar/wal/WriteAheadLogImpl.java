package com.arorasagar.wal;

import com.arorasagar.wal.exception.WALException;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

public class WriteAheadLogImpl {

    private static final int MAX_RECORD_SIZE = 10 * 1024 * 1024;
    private final AtomicLong sequence = new AtomicLong();
    private final WALConfig walConfig;
    private File file;
    private BufferedOutputStream bufferedOutputStream;

    public WriteAheadLogImpl() throws IOException {
        this(WALConfig.builder().build());
    }

    public WriteAheadLogImpl(WALConfig walConfig) throws IOException {
        this.walConfig = walConfig;
        long currentTimestamp = System.currentTimeMillis();
        String fileName = "wal-" + currentTimestamp + ".log";
        Path filePath = Paths.get(walConfig.getDirName(), fileName);
        open(filePath);
    }

    public void open(Path path) throws IOException {
        file = path.toFile();
        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
    }

    void close() {

    }

    void load(File dir) {

    }

    // TODO:Add exception
    private void checkSize(byte[] key) throws WALException {

    }

    public void operation(EntryType entryType, byte[] key, byte[] val) throws WALException {
        checkSize(key);
        checkSize(val);


    }

    void flush() {

    }
}
