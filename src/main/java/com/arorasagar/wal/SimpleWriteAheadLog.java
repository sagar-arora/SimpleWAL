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

    private static final int MAX_RECORD_SIZE = 10 * 1024 * 1024;
    private final AtomicLong sequence = new AtomicLong();
    private final WALConfig walConfig;
    private File file;
    private BufferedOutputStream bufferedOutputStream;

    public SimpleWriteAheadLog() throws IOException {
        this(WALConfig.builder().build());
    }

    public SimpleWriteAheadLog(WALConfig walConfig) throws IOException {
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

    void close() throws IOException {
        bufferedOutputStream.close();
    }

    List<WALEntry> load() throws IOException {
        List<Path> paths = FileUtils.listLogFiles(Paths.get(walConfig.getDirName()));

        List<WALEntry> entries = new ArrayList<>();
        for (Path path : paths) {
            System.out.println(path.toString());
            File file = path.toFile();
            WALIterator walIterator = new WALIterator(file);
            if (walIterator.hasNext()) {
                entries.add(walIterator.next());
            }
        }
        FileUtils.cleanup(paths);
        return entries;
    }

    // TODO:Add exception
    private void checkSize(byte[] key) throws WALException {

    }

    public void operation(EntryType entryType, byte[] key, byte[] val) throws WALException, IOException {

        checkSize(key);
        checkSize(val);
        long timestamp = System.currentTimeMillis();

        long keySize = key.length;
        long valSize = val.length;
        ByteBuffer buffer = ByteBuffer.allocate(4 + 1 + 8 + (int) keySize + 8 + (int) valSize + 8);
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