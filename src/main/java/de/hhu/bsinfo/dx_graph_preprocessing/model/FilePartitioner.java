package de.hhu.bsinfo.dx_graph_preprocessing.model;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;

public class FilePartitioner {

    private List<Short> nodeIds;
    private int lastLineSize;
    private RandomAccessFile raf;
    private long lastOffset = 0;
    private long lastId = Integer.MIN_VALUE;

    public FilePartitioner(List<Short> nodeIds, int lastLineSize) {
        this.nodeIds = nodeIds;
        this.lastLineSize = lastLineSize;
    }

    public HashMap<Short, Partition> determinePartitions(String filePath, int numberOfProperties) {
        HashMap<Short, Partition> partitions = new HashMap<>();
        try {
            File file = new File(filePath);
            raf = new RandomAccessFile(file, "r");

            long fileSize = raf.length();

            String firstLine = raf.readLine();
            lastId = Long.parseLong(firstLine.split("\\s")[0]);

            raf.seek(fileSize / nodeIds.size());


            for (int i = 0; i < nodeIds.size(); i++) {
                Partition partition = new Partition(lastOffset, nodeIds.get(i));
                partition.setStartVertex(lastId);

                if (i != 0 && i < nodeIds.size() - 1) {
                    raf.seek((raf.length() / nodeIds.size()) * (i + 1));
                }
                if (i == nodeIds.size() - 1) {
                    partition.setEndByteOffset(raf.length());
                    raf.seek(raf.length() - lastLineSize);
                    partition.setEndVertex(Long.parseLong(raf.readLine().split("\\s")[0]));
                    partitions.put(nodeIds.get(i), partition);
                    System.out.println(partition.toString());
                    continue;
                }
                String line = raf.readLine(); //can be uncompleted line because of jump
                line = raf.readLine(); //here definitely complete line

                String[] split = line.split("\\s");
                lastId = Long.parseLong(split[0]);

                //search for border
                long source = 0;
                while (true) {
                    line = raf.readLine();
                    split = line.split("\\s");
                    source = Long.parseLong(split[0]);
                    if (source != lastId) {
                        lastOffset = raf.getFilePointer() - line.length() - 1;
                        break;
                    }
                }
                partition.setEndByteOffset(lastOffset);
                partition.setEndVertex(lastId);
                System.out.println(partition.toString());
                System.out.println(lastId);
                lastId = source;
                partitions.put(nodeIds.get(i), partition);
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return partitions;
    }
}
