package de.hhu.bsinfo.dx_graph_preprocessing.model;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class RandomAccessReader extends Reader {
    private static final Logger LOGGER = LogManager.getFormatterLogger(RandomAccessReader.class);

    private Partition partition;

    public RandomAccessReader(InputProcessor processor, Partition p) {
        super(processor);
        this.partition = p;
    }

    @Override
    public void readFile(String path) {
        try {
            File file = new File(path);
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            FileChannel channel = raf.getChannel();
            long numberOfBytesToRead = partition.getEndByteOffset() - partition.getStartByteOffset();
            MappedByteBuffer byteBuffer;
            processor.init();
            if (numberOfBytesToRead <= Integer.MAX_VALUE) {
                byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, partition.getStartByteOffset(), numberOfBytesToRead);
                processBuffer(byteBuffer);
            } else {
                int iterations = (int) Math.ceil(numberOfBytesToRead / (double) Integer.MAX_VALUE);
                LOGGER.info("Buffer can not load partition on one. Partition will be load in %d iterations", iterations);
                int iByteRange = 0;
                for (int i = 0; i < iterations; i++) {
                    long iOffset = partition.getStartByteOffset() + iByteRange;
                    iByteRange = i != iterations - 1 ? Integer.MAX_VALUE : (int) numberOfBytesToRead - (i * Integer.MAX_VALUE);
                    LOGGER.info("Byterange: %d", iByteRange);
                    byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, iOffset, iByteRange);
                    processBuffer(byteBuffer);
                }
            }
            processor.close();
            channel.close();
            raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processBuffer(MappedByteBuffer buffer) {
        buffer.load();
        StringBuilder sb = new StringBuilder();
        int limit = buffer.limit();
        for (int i = 0; i < limit; i++) {
            char c = (char) buffer.get();

            sb.append(c);
            if (c == '\n') {
                processor.processReadLine(sb.toString());
                sb.setLength(0);
            }

        }
    }
}
