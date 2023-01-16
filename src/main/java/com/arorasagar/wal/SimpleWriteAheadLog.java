package com.arorasagar.wal;

import com.arorasagar.wal.exception.WALException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    public WALEntry deserialize(ByteBuffer buffer) {
        int position = 0;
        int index = buffer.getInt();
        byte entryType = buffer.get();
        long keySize = buffer.getLong();
        byte[] key = new byte[(int) keySize];
        buffer.get(key, 0, (int) keySize);
        long valSize = buffer.getLong();
        byte[] val = new byte[(int) valSize];
        buffer.get(val, 0, (int) valSize);
        long timestamp = buffer.getLong();

        return WALEntry.builder()
                .key(key)
                .value(val)
                .index(index)
                .entryType(EntryType.getCommandFromVal(entryType))
                .timestamp(timestamp)
                .build();
    }

    List<WALEntry> load() throws IOException {

        List<FileMetadata> fileMetadataList = FileUtils.listLogFiles(Paths.get(walConfig.getDirName()));

        List<WALEntry> entries = new ArrayList<>();

        for (FileMetadata fileMetadata : fileMetadataList) {
            File file = fileMetadata.getFilePath().toFile();
            WALIterator walIterator = new WALIterator(file);
            if (walIterator.hasNext()) {
                entries.add(walIterator.next());
            }
        }
        // first close any streams that are open
        close();
        FileUtils.cleanup(fileMetadataList);
        return entries;
    }

    private Path logName(long currentTimestamp) {
        String fileName = "wal-" + currentTimestamp + ".log";
        return Paths.get(walConfig.getDirName(), fileName);
    }

    private void rollover() throws IOException {
        Path logFile = logName(System.currentTimeMillis());

        if (bufferedOutputStream != null) {
            bufferedOutputStream.flush();
        }

        open(logFile);
    }

    public void operation(EntryType entryType, byte[] key, byte[] val) throws WALException, IOException {
        long timestamp = System.currentTimeMillis();

        if (key == null) {
            key = new byte[0];
        }
        if (val == null) {
            val = new byte[0];
        }
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

    public static final class SimpleWriteAheadLogWriter {

        private final AtomicLong sequence = new AtomicLong();
        private final WALConfig walConfig;
        private BufferedOutputStream bufferedOutputStream;

        public SimpleWriteAheadLogWriter() {
            this(WALConfig.builder().build());
        }

        public SimpleWriteAheadLogWriter(WALConfig walConfig) {
            this.walConfig = walConfig;
        }

        public void open() throws IOException {
            Path logFile = logName(System.currentTimeMillis());
            open(logFile);
        }

        public void open(Path path) throws IOException {
            open(path.toFile());
        }

        public void open(File file) throws IOException {
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
        }

        void close() throws IOException {
            bufferedOutputStream.close();
        }

        private Path logName(long currentTimestamp) {
            String fileName = "wal-" + currentTimestamp + ".log";
            return Paths.get(walConfig.getDirName(), fileName);
        }

        private void rollover() throws IOException {
            Path logFile = logName(System.currentTimeMillis());

            if (bufferedOutputStream != null) {
                flush();
            }

            open(logFile);
        }

        public byte[] searlize(EntryType entryType, byte[] key, byte[] val, long timestamp) throws IOException {
            if (key == null) {
                key = new byte[0];
            }
            if (val == null) {
                val = new byte[0];
            }
            long keySize = key.length;
            long valSize = val.length;
            int recordSize = 4 + 1 + 8 + (int) keySize + 8 + (int) valSize + 8;

            // TODO: add check and throw Exception
            if (recordSize > walConfig.getMaxRecordSize()) {

            }

            if (recordSize > walConfig.getMaxFileSize()) {
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

            return buffer.array();
        }

        public void write(EntryType entryType, byte[] key, byte[] val, long timestamp) throws WALException, IOException {
            bufferedOutputStream.write(searlize(entryType, key, val, timestamp));
        }

        void flush() throws IOException {
            bufferedOutputStream.flush();
        }
    }

}
