package de.hhu.bsinfo.dx_graph_preprocessing;

import de.hhu.bsinfo.dx_graph_preprocessing.model.*;
import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static de.hhu.bsinfo.dx_graph_preprocessing.Util.getNodeIndex;


public class DxGraphPreprocessingApplication extends Application {

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "DxGraphPreprocessing";
    }

    @Override
    public void main(String[] args) {
        int i = 0;
        String filePath = args[i++];
        String graphName = args[i++];
        String outPath = args[i++];
        int numberOfProperties = Integer.parseInt(args[i++]);
        int numberOfVertex = Integer.parseInt(args[i++]);
        float fillFactor = Float.parseFloat(args[i++]);
        int lastLineSize = Integer.parseInt(args[i++]);

        BootService bootService = this.getService(BootService.class);
        List<Short> nodeIds = bootService.getOnlinePeerNodeIDs();
        short currentNodeId = bootService.getNodeID();

        long partitionTime = 0;
        long vertexTime = 0;
        long edgeTime = 0;

        long startTime = System.nanoTime();

        FilePartitioner filePartitioner = new FilePartitioner(nodeIds, lastLineSize);

        long stopTime = System.nanoTime();
        partitionTime = stopTime - startTime;

        startTime = System.nanoTime();
        HashMap<Short, Partition> partitions = filePartitioner.determinePartitions(filePath + graphName + ".e", numberOfProperties);

        LDBCVertexPreprocessor vertexPreprocessor = new LDBCVertexPreprocessor(outPath, graphName, numberOfVertex, fillFactor, partitions.get(currentNodeId), currentNodeId);
        WholeFileReader wholeFileReader = new WholeFileReader(vertexPreprocessor);
        wholeFileReader.readFile(filePath + graphName + ".v");

        stopTime = System.nanoTime();
        vertexTime = stopTime - startTime;

        startTime = System.nanoTime();
        LDBCEdgePreprocessor edgePreprocessor = new LDBCEdgePreprocessor(outPath, graphName, vertexPreprocessor.getIdMapper(), currentNodeId);
        RandomAccessReader reader = new RandomAccessReader(edgePreprocessor, partitions.get(currentNodeId));
        reader.readFile(filePath + graphName + ".e");

        stopTime = System.nanoTime();
        edgeTime = stopTime - startTime;

        System.out.println(String.format("Partition time: %.2f", partitionTime / 1e+9));
        System.out.println(String.format("Vertex time: %.2f", vertexTime / 1e+9));
        System.out.println(String.format("Edge time: %.2f", edgeTime / 1e+9));
        System.out.println(String.format("Whole time: %.2f", (partitionTime + vertexTime + edgeTime) / 1e+9));

        File file = new File(outPath + graphName + "_results.csv");
        try {
            FileWriter fw = new FileWriter(file, true);
            fw.write(currentNodeId +";");
            fw.write(partitionTime / 1e+9 + ";");
            fw.write(vertexTime / 1e+9 + ";");
            fw.write(edgeTime / 1e+9 + ";");
            fw.write((partitionTime + vertexTime + edgeTime) / 1e+9 + "\n");
        fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void signalShutdown() {

    }
}
