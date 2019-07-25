package de.hhu.bsinfo.dx_graph_preprocessing;

import de.hhu.bsinfo.dx_graph_preprocessing.model.*;
import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static de.hhu.bsinfo.dx_graph_preprocessing.Util.getNodeIndex;


public class DxGraphPreprocessingApplication extends Application {

    private final Logger LOGGER = LogManager.getFormatterLogger(DxGraphPreprocessingApplication.class);


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
        long memoryToAllocate = Long.parseLong(args[i++]);
        long memoryToIncrement = Long.parseLong(args[i++]);
        int numberOfProperties = Integer.parseInt(args[i++]);
        int numberOfVertex = Integer.parseInt(args[i++]);

        BootService bootService = this.getService(BootService.class);
        List<Short> nodeIds = bootService.getOnlinePeerNodeIDs();
        short currentNodeId = bootService.getNodeID();
        FilePartitioner filePartitioner = new FilePartitioner(nodeIds);
        List<Partition> partitions = filePartitioner.determinePartitions(filePath + graphName + ".e", numberOfProperties);

        LDBCVertexPreprocessor vertexPreprocessor = new LDBCVertexPreprocessor(outPath, graphName, numberOfVertex);
        WholeFileReader wholeFileReader = new WholeFileReader(vertexPreprocessor);
        wholeFileReader.readFile(filePath + graphName + ".v");

        LDBCEdgePreprocessor edgePreprocessor = new LDBCEdgePreprocessor(outPath, graphName, vertexPreprocessor.getIdMapper(), memoryToAllocate, memoryToIncrement, getNodeIndex(nodeIds, currentNodeId));
        RandomAccessReader reader = new RandomAccessReader(edgePreprocessor, partitions.get(getNodeIndex(nodeIds, currentNodeId)));
        reader.readFile(filePath + graphName + ".e");
        //reader.readFile(filePath + graphName + "e", false);
    }

    @Override
    public void signalShutdown() {

    }
}
