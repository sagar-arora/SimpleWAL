package com.arorasagar.wal;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

public class WALIterator implements Iterator<WALEntry> {

    private ByteBuffer buffer;

    public WALIterator(File file) {
        try (RandomAccessFile aFile = new RandomAccessFile(file.getName(), "r");
             FileChannel inChannel = aFile.getChannel();) {

            long fileSize = inChannel.size();
            buffer = ByteBuffer.allocate((int)fileSize);
            inChannel.read(buffer);
            buffer.flip();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public WALIterator(byte[] fileBytes) {
        buffer = ByteBuffer.wrap(fileBytes);
    }


    @Override
    public boolean hasNext() {
        return buffer.position() < buffer.capacity();
    }

    @Override
    public WALEntry next() {
        int index = buffer.getInt();
        byte entryType = buffer.get();
        long keySize = buffer.getLong();
        System.out.println("keysize" + keySize);
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

    @Override
    public void remove() {

    }
}
