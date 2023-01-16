package com.arorasagar.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SimpleWriteAheadLogReader {
    
    File file;
    ByteBuffer buffer;
    
    public SimpleWriteAheadLogReader(File file) {
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
    
    public void read() {
        int index = buffer.getInt();
        byte entryType = buffer.get();
        long keySize = buffer.getLong();
        byte[] key = new byte[(int) keySize];
        buffer.get(key, 0, (int) keySize);
        long valSize = buffer.getLong();
        byte[] val = new byte[(int) valSize];
        buffer.get(val, 0, (int) valSize);
        long timestamp = buffer.getLong();
    }
}
