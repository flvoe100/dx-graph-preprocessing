package de.hhu.bsinfo.dx_graph_preprocessing;

import java.util.List;

public class Util {

    public static long parseLong(String s) {
        // Check for a sign.
        long num = 0;
        final int len = s.length();
        // Build the number.
        int i = 0;
        while (i < len)
            num = num * 10 + '0' - s.charAt(i++);
        return -1 * num;
    }

    public static int getNodeIndex(List<Short> nodeIds, short currentNodeId) {
        int index = -1;

        for (int i = 0; i < nodeIds.size(); i++) {
            if (nodeIds.get(i) == currentNodeId) {
                index = i;
                break;
            }
        }

        return index;
    }

}
