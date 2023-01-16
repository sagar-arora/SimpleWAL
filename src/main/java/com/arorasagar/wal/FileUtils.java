package com.arorasagar.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class FileUtils {

    private static final Pattern pattern = Pattern.compile("wal-[0-9]*\\.log");

    public static List<FileMetadata> listLogFiles(Path dir) throws IOException {
        return Files.list(dir)
                .filter((Path path) -> pattern.matcher(path.getFileName().toString()).find())
                .map((Path path) -> {
                    int startIndex = path.getFileName().toString().indexOf("wal-");
                    int endIndex = path.getFileName().toString().indexOf(".log");
                    return FileMetadata
                            .builder()
                            .filePath(path)
                            .timestamp(Long.parseLong(path.getFileName().toString().substring(startIndex + 4, endIndex)))
                            .build();
                })
                .collect(Collectors.toList());
    }

    public static List<FileMetadata> sortFilesByTimestamp(List<FileMetadata> paths) {
        Collections.sort(paths);
        return paths;
    }

    public static void cleanup(List<FileMetadata> paths) {
        for (FileMetadata path : paths) {
            path.getFilePath().toFile().delete();
        }
    }
}
