package de.hhu.bsinfo.dx_graph_preprocessing.model;

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileOutputStream;
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
    private Partition p;
    private int currentNodeId;


    public LDBCVertexPreprocessor(String outPath, String datasetPrefix, int cntVertices, float fillFactor, Partition p, int currentNodeId) {
        this.cntVertices = 0;
        this.outPath = outPath;
        this.datasetPrefix = datasetPrefix;
        this.idMapper = new Long2IntOpenHashMap(cntVertices, fillFactor);
        this.p = p;
        this.currentNodeId = currentNodeId ;
    }

    @Override
    public void init() throws IOException {
        this.bw = new FastBufferedOutputStream(new FileOutputStream(outPath + datasetPrefix + ".out." + currentNodeId + ".v"));
        LOGGER.info("Start reading and writing vertex data part %d", currentNodeId);
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
        if (vid >= p.getStartVertex() && vid <= p.getEndVertex()) {
            try {
                bw.write((vid + "\n").getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (cntVertices % outMod == 0) {
            System.out.println(String.format("Processing: %dM vertices finished...", (cntVertices / outMod)));
        }
    }


    public Long2IntOpenHashMap getIdMapper() {
        return idMapper;
    }
}
