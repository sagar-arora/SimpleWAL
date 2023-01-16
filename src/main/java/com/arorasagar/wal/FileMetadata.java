package com.arorasagar.wal;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.nio.file.Path;

@Getter
@Builder
@ToString
public class FileMetadata implements Comparable<FileMetadata> {
    private Path filePath;
    private long timestamp;

    @Override
    public int compareTo(FileMetadata f) {
        return Long.compare(f.getTimestamp(), this.timestamp);
    }
}
