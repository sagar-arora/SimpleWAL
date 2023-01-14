package com.arorasagar.wal;

import com.arorasagar.wal.exception.WALException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleWriteAheadLog {

    private static final int MAX_FILE_SIZE = 30 * 1024 * 1024;
    private static final int MAX_RECORD_SIZE = 10 * 1024 * 1024;
    private final AtomicLong sequence = new AtomicLong();
    private final WALConfig walConfig;
    private BufferedOutputStream bufferedOutputStream;

    public SimpleWriteAheadLog() {
        this(WALConfig.builder().build());
    }

    public SimpleWriteAheadLog(WALConfig walConfig) {
        this.walConfig = walConfig;
    }

    public void open() throws IOException {
        Path logFile = logName(System.currentTimeMillis());
        open(logFile);
    }

    public void open(Path path) throws IOException {
        File file = path.toFile();
        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
    }

    void close() throws IOException {
        bufferedOutputStream.close();
    }

    List<WALEntry> load() throws IOException {
        List<Path> paths = FileUtils.listLogFiles(Paths.get(walConfig.getDirName()));

        List<WALEntry> entries = new ArrayList<>();

        for (Path path : paths) {
            File file = path.toFile();
            WALIterator walIterator = new WALIterator(file);
            if (walIterator.hasNext()) {
                entries.add(walIterator.next());
            }
        }

        // first close any streams that are open
        close();
        FileUtils.cleanup(paths);
        return entries;
    }


    public Path logName(long currentTimestamp) {
        String fileName = "wal-" + currentTimestamp + ".log";
        return Paths.get(walConfig.getDirName(), fileName);
    }

    // TODO:Add exception
    private void checkSize(byte[] key) throws WALException {

    }

    public void rollover() throws IOException {
        Path logFile = logName(System.currentTimeMillis());

        if (bufferedOutputStream != null) {
            bufferedOutputStream.flush();
        }

        open(logFile);
    }

    public void operation(EntryType entryType, byte[] key, byte[] val) throws WALException, IOException {

        checkSize(key);
        checkSize(val);
        long timestamp = System.currentTimeMillis();

        long keySize = key.length;
        long valSize = val.length;
        int recordSize = 4 + 1 + 8 + (int) keySize + 8 + (int) valSize + 8;

        if (recordSize > MAX_RECORD_SIZE) {

        }

        if (recordSize > MAX_FILE_SIZE) {
            rollover();
        }

        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putInt((int) sequence.incrementAndGet());
        buffer.put(entryType.getB());
        buffer.putLong(keySize);
        buffer.put(key);
        buffer.putLong(valSize);
        buffer.put(val);
        buffer.putLong(timestamp);

        bufferedOutputStream.write(buffer.array());
    }

    void flush() throws IOException {
        bufferedOutputStream.flush();
    }
}
