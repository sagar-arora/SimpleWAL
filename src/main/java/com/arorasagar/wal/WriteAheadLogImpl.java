package com.arorasagar.wal;

import com.arorasagar.wal.exception.WALException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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

    void close() throws IOException {
        bufferedOutputStream.close();
    }

    void load() throws IOException {
        List<Path> paths = FileUtils.listLogFiles(Paths.get(walConfig.getDirName()));
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
        buffer.flip();
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
