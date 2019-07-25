package de.hhu.bsinfo.dx_graph_preprocessing.model;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import static de.hhu.bsinfo.dx_graph_preprocessing.Util.parseLong;

public class LDBCEdgePreprocessor implements InputProcessor {

    private static final Logger LOGGER = LogManager.getFormatterLogger(LDBCEdgePreprocessor.class);

    private String graphName;
    private String outPath;
    /*    private DB db;
        private DB.TreeMapSink<Long, Integer> sink;
        private BTreeMap<Long, Integer> idMapper;
     */
    private Map<Long, Integer> idMapper;
    private int cntEdges;
    private int currentNodeIndex;
    final int outMod = 10_000_000;
    private BufferedWriter bw;
    private long lastID = 0;
    private long lastIDnew = 0;


    public LDBCEdgePreprocessor(String outPath, String graphName, Map<Long, Integer> idMapper, long memoryToAllocate, long memoryToIncrement, int currentNodeIndex) {
        this.graphName = graphName;
        this.outPath = outPath;
        this.cntEdges = 0;
        this.currentNodeIndex = currentNodeIndex +1;
        lastID = 0;
        lastIDnew = 0;
        this.idMapper = idMapper;
        /*
        this.db = DBMaker.memoryDB().allocateStartSize(memoryToAllocate)
                .allocateIncrement(memoryToIncrement)
                .make();
        this.sink = db.treeMap("idMapper")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.INTEGER)
                .createFromSink();

         */
    }

    @Override
    public void init() throws IOException {
        bw = new BufferedWriter(new FileWriter(outPath + graphName + ".out." + currentNodeIndex + ".e"), 100_000_000);
        LOGGER.info("Start reading and writing edge data");


    }

    @Override
    public void close() throws IOException {
        //  this.idMapper = sink.create();
        // this.sink = null;
        //System.gc();
        bw.flush();
        LOGGER.info("Finished reading and writing edge data");
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
            bw.write(lastIDnew + " " + idMapper.get(right) +"\n");
            cntEdges++;
            if (cntEdges % outMod == 0) {
                System.out.println(String.format("Processing: %d0M edges finished...", (cntEdges / outMod)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
