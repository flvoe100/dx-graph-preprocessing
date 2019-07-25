package de.hhu.bsinfo.dx_graph_preprocessing.model;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class WholeFileReader extends Reader {


    public WholeFileReader(InputProcessor processor) {
        super(processor);
    }

    @Override
    public void readFile(String path) {
        try {
            final BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            new BufferedInputStream(
                                    Files.newInputStream(Paths.get(path), StandardOpenOption.READ),
                                    1000000),
                            StandardCharsets.US_ASCII));

            String line = null;
            processor.init();
            while ((line = br.readLine()) != null) {

                processor.processReadLine(line);

            }
            System.gc();
            processor.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
