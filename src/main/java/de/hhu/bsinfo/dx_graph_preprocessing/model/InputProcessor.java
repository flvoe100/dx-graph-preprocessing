package de.hhu.bsinfo.dx_graph_preprocessing.model;

import java.io.IOException;

interface InputProcessor {

     void init() throws IOException;

     void close() throws IOException;

     void processReadLine(String line);
}
