package com.arorasagar.wal;

public enum EntryType {
    SET((byte) 0), DELETE((byte) 1);

    private final byte b;

    EntryType(byte b) {
        this.b = b;
    }

    public static EntryType getCommandFromVal(byte b) {
        EntryType ans = EntryType.SET;
        switch (b) {
             case (byte) 0:
                 ans = EntryType.SET;
                 break;
             case 1:
                 ans = EntryType.DELETE;
                 break;
        }

        return ans;
    }
}
