package de.hhu.bsinfo.dx_graph_preprocessing.model;


import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileOutputStream;
import java.io.IOException;

import static de.hhu.bsinfo.dx_graph_preprocessing.Util.parseLong;

public class LDBCEdgePreprocessor implements InputProcessor {

    private static final Logger LOGGER = LogManager.getFormatterLogger(LDBCEdgePreprocessor.class);

    private String graphName;
    private String outPath;

    private int cntEdges;
    private int currentNodeId;
    final int outMod = 10_000_000;
    private FastBufferedOutputStream bw;
    private long lastID = 0;
    private long lastIDnew = 0;
    private Long2IntOpenHashMap idMapper;

    public LDBCEdgePreprocessor(String outPath, String graphName, Long2IntOpenHashMap idMapper, int currentNodeId) {
        this.graphName = graphName;
        this.outPath = outPath;
        this.cntEdges = 0;
        this.currentNodeId = currentNodeId;
        lastID = 0;
        lastIDnew = 0;
        this.idMapper = idMapper;

    }

    @Override
    public void init() throws IOException {
        this.bw = new FastBufferedOutputStream(new FileOutputStream(outPath + graphName + ".out." + currentNodeId + ".e"));
        //bw = new BufferedWriter(new FileWriter(outPath + graphName + ".out." + currentNodeIndex + ".e"), 100_000_000);
        LOGGER.info("Start reading and writing edge data");

    }

    @Override
    public void close() throws IOException {
        bw.flush();
        LOGGER.info("Finished reading and writing edge data. Read %d edges.", this.cntEdges);
        bw.close();
    }

    @Override
    public void processReadLine(String line) {
        String[] split = line.split("\\s");
        long left = parseLong(split[0]);
        long right = parseLong(split[1]);
        if (lastID != left) {
            lastIDnew = idMapper.get(left);
        }
        try {
            // bw.write((lastIDnew + " " + idMapper.get(right) +"\n").getBytes());
            bw.write((left + " " + right + "\n").getBytes());
            cntEdges++;
            if (cntEdges % outMod == 0) {
                System.out.println(String.format("Processing: %d0M edges finished...", (cntEdges / outMod)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
