package org.seaborne.delta;

import org.apache.jena.dboe.base.file.BinaryDataFile;

import java.util.UUID;

public class UUIDHandler {
    /** Quick test of whether a string looks like an UUID or not */
    public static boolean maybeUUID(String str) {
        String nilStr = "00000000-0000-0000-0000-000000000000";
        return str.length() == nilStr.length() && str.charAt(8)=='-';
    }

    /** Parse a UUID string, return a default if it does not parse correctly */
    public static UUID parseUUID(String patchStr, UUID dft) {
        try {
            return UUID.fromString(patchStr);
        } catch (IllegalArgumentException ex) {
            return dft;
        }
    }

    public static String shortUUIDstr(UUID uuid) {
        String str = uuid.toString();
        int version = uuid.version();
        if ( version == 1 )
            // Type 1 : include varying part! xxxx-yyyy
            // 19-28 is
            //return str.substring(19, 28);
            // 0-6 is the low end of the clock.
            return uuid.toString().substring(0,6);
        if ( version == 4 )
            // Type 4 - use the first few hex characters.
            return uuid.toString().substring(0,6);
        return uuid.toString().substring(0,8);
    }
}
