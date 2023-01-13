package com.arorasagar.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public final class FileUtils {

    public static List<Path> listLogFiles(Path dir) throws IOException {
        return Files.list(dir).toList();
    }
}
