package com.arorasagar.wal;

import java.io.File;
import java.util.Iterator;

public interface WriteAheadLog {

    void openWAL(File file);

    Iterator<WALIterator> readAll();


}
