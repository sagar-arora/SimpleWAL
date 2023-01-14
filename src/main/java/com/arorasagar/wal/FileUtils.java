package com.arorasagar.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class FileUtils {

    private static final Pattern pattern = Pattern.compile("wal-[0-9]*\\.log");

    public static List<Path> listLogFiles(Path dir) throws IOException {
        return Files.list(dir).filter((Path path) -> pattern.matcher(path.getFileName().toString()).find()).collect(Collectors.toList());
    }

    public static void cleanup(List<Path> paths) {
        for (Path path : paths) {
            System.out.println("Trying to delete: " + path);
            if (path.toFile().delete()) {
                System.out.println("deleted");

            }
        }
    }
}
