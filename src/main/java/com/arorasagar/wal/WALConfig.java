package com.arorasagar.wal;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.nio.file.Paths;

@Builder
@Getter
@Setter
public class WALConfig {
    @Builder.Default
    private String dirName = Paths.get("").toAbsolutePath().toString();
    @Builder.Default
    private long maxFileSize = 30 * 1024 * 1024;
    @Builder.Default
    private long maxRecordSize = 10 * 1024 * 1024;
}
