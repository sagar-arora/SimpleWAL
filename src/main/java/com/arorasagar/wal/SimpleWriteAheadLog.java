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

    private final WALConfig walConfig;
    private final SimpleWriteAheadLogWriter writer;

    public SimpleWriteAheadLog() throws IOException {
        this(WALConfig.builder().build());
    }

    public SimpleWriteAheadLog(WALConfig walConfig) throws IOException {
        this.walConfig = walConfig;
        File file = open();
        writer = new SimpleWriteAheadLogWriter(file);
    }

    public File open() throws IOException {
        Path logFile = logName(System.currentTimeMillis());
        return open(logFile);
    }

    public File open(Path path) throws IOException {
        return path.toFile();
    }

    public WALEntry deserialize(ByteBuffer buffer) {
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

    public byte[] searlize(long sequence, EntryType entryType, byte[] key, byte[] val, long timestamp) throws IOException {
        if (key == null) {
            key = new byte[0];
        }
        if (val == null) {
            val = new byte[0];
        }
        long keySize = key.length;
        long valSize = val.length;
        int recordSize = 4 + 1 + 8 + (int) keySize + 8 + (int) valSize + 8;

        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putInt((int) sequence);
        buffer.put(entryType.getB());
        buffer.putLong(keySize);
        buffer.put(key);
        buffer.putLong(valSize);
        buffer.put(val);
        buffer.putLong(timestamp);

        return buffer.array();
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
        writer.close();
        FileUtils.cleanup(fileMetadataList);
        return entries;
    }

    private Path logName(long currentTimestamp) {
        String fileName = "wal-" + currentTimestamp + ".log";
        return Paths.get(walConfig.getDirName(), fileName);
    }

    public void operation(EntryType entryType, byte[] key, byte[] val, long timestamp) throws WALException, IOException {
        writer.write(entryType, key, val, timestamp);
    }

    public void flush() throws IOException {
        writer.flush();
    }


    public final class SimpleWriteAheadLogWriter {

        private final AtomicLong sequence = new AtomicLong();
        private final OutputStream outputStream;

        public SimpleWriteAheadLogWriter(File file) throws FileNotFoundException {
            outputStream = new BufferedOutputStream(new FileOutputStream(file));
        }

        void close() throws IOException {
            outputStream.close();
        }

        private void rollover() throws IOException {
            Path logFile = logName(System.currentTimeMillis());
            flush();
            open(logFile);
        }


        public void write(EntryType entryType, byte[] key, byte[] val, long timestamp) throws WALException, IOException {
            byte[] bytes = searlize(sequence.incrementAndGet(), entryType, key, val, timestamp);
            int recordSize = bytes.length;
            // TODO: add check and throw Exception
            if (recordSize > walConfig.getMaxRecordSize()) {

            }

            if (recordSize > walConfig.getMaxFileSize()) {
                rollover();
            }

            outputStream.write(bytes);
        }

        void flush() throws IOException {
            outputStream.flush();
        }
    }

}
