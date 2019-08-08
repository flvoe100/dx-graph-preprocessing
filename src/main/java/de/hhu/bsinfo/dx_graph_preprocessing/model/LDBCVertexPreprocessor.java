package de.hhu.bsinfo.dx_graph_preprocessing.model;

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;


import static de.hhu.bsinfo.dx_graph_preprocessing.Util.parseLong;

public class LDBCVertexPreprocessor implements InputProcessor {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LDBCEdgePreprocessor.class);

    // private Map<Long, Integer> idMapper;
    private String outPath;
    private String datasetPrefix;
    private int cntVertices;
    final int outMod = 1_000_000;
    private FastBufferedOutputStream bw;
    private Long2IntOpenHashMap idMapper;


    public LDBCVertexPreprocessor(String outPath, String datasetPrefix, int cntVertices, int fillFactor ) {
        this.cntVertices = 0;
        this.outPath = outPath;
        this.datasetPrefix = datasetPrefix;
        this.idMapper = new Long2IntOpenHashMap(cntVertices, fillFactor);
    }

    @Override
    public void init() throws IOException {
        this.bw = new FastBufferedOutputStream(new FileOutputStream(outPath + datasetPrefix +"out.v"));
        //this.bw = new BufferedWriter(new FileWriter(outPath + datasetPrefix + ".out.v"), 100_000_000);
        LOGGER.info("Start reading and writing vertex data");
    }

    @Override
    public void close() throws IOException {

        System.gc();
        bw.close();
        LOGGER.info("Finished reading and writing vertex data");

    }

    @Override
    public void processReadLine(String line) {
        long vid = parseLong(line.split("\\s")[0]);
        cntVertices++;

        idMapper.put(vid, cntVertices);
        try {

            bw.write((cntVertices + "\n").getBytes());

            if (cntVertices % outMod == 0) {
                System.out.println(String.format("Processing: %dM vertices finished...", (cntVertices / outMod)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public Long2IntOpenHashMap getIdMapper() {
        return idMapper;
    }
}
