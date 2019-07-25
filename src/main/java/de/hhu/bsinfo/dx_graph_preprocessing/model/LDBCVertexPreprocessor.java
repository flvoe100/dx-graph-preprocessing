package de.hhu.bsinfo.dx_graph_preprocessing.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static de.hhu.bsinfo.dx_graph_preprocessing.Util.parseLong;

public class LDBCVertexPreprocessor implements InputProcessor {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LDBCEdgePreprocessor.class);

    private Map<Long, Integer> idMapper;
    private String outPath;
    private String datasetPrefix;
    private int cntVertices;
    final int outMod = 1_000_000;
    private BufferedWriter bw;


    public LDBCVertexPreprocessor(String outPath, String datasetPrefix, int size) {
        this.idMapper = new HashMap<>(size);
        this.cntVertices = 0;
        this.outPath = outPath;
        this.datasetPrefix = datasetPrefix;

    }

    @Override
    public void init() throws IOException {
        this.bw = new BufferedWriter(new FileWriter(outPath + datasetPrefix + ".out.v"), 100_000_000);
        LOGGER.info("Start reading and writing vertex data");
    }

    @Override
    public void close() throws IOException {
        bw.close();
        LOGGER.info("Finished reading and writing vertex data");

    }

    @Override
    public void processReadLine(String line) {
        long vid = parseLong(line.split("\\s")[0]);
        cntVertices++;
        idMapper.put(vid, cntVertices);
        try {
            bw.write(cntVertices + "\n");

            if (cntVertices % outMod == 0) {
                System.out.println(String.format("Processing: %dM vertices finished...", (cntVertices / outMod)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public Map<Long, Integer> getIdMapper() {
        return idMapper;
    }
}
