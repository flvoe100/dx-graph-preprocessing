package de.hhu.bsinfo.dx_graph_preprocessing.model;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static de.hhu.bsinfo.dx_graph_preprocessing.Util.parseLong;

public class FilePartitioner {

    private List<Short> nodeIds;

    public FilePartitioner(List<Short> nodeIds) {
        this.nodeIds = nodeIds;
    }

    public List<Partition> determinePartitions(String filePath, int numberOfProperties) {
        ArrayList<Partition> partitions = new ArrayList<>();
        try {
            File file = new File(filePath);
            RandomAccessFile raf = new RandomAccessFile(file, "r");

            long fileSize = raf.length();
            long lastId = Integer.MIN_VALUE;
            long lastOffset = 0;
            raf.seek(fileSize / nodeIds.size());


            for (int i = 0; i < nodeIds.size(); i++) {
                Partition partition = new Partition(lastOffset, nodeIds.get(i));
                if (i != 0) {
                    raf.seek((raf.length() / nodeIds.size()) * (i + 1));
                }
                if (i == nodeIds.size() -1) {
                    partition.setEndByteOffset(raf.length());
                    partitions.add(partition);
                    System.out.println(partition.toString());
                    continue;
                }
                String line = raf.readLine();
                String[] split = line.split("\\s");

                if (split.length != 1 + 2 || split[0].length() == 0 || parseLong(split[0]) < lastId) {

                    line = raf.readLine();

                    split = line.split("\\s");
                }

                lastId = Long.parseLong(split[0]);
                while (true) {
                    line = raf.readLine();
                    //System.out.println(line);
                    split = line.split("\\s");
                    long source = Long.parseLong(split[0]);
                    if (source != lastId) {
                        lastOffset = raf.getFilePointer() - line.length() - 1;
                        break;
                    }
                }
                partition.setEndByteOffset(lastOffset);
                System.out.println(partition.toString());
                System.out.println(lastId);
                partitions.add(partition);
            }



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return partitions;
    }


}
