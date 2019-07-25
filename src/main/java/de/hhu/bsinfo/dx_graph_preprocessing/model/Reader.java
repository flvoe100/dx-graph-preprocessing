package de.hhu.bsinfo.dx_graph_preprocessing.model;


abstract class Reader {

    InputProcessor processor;


    public Reader(InputProcessor processor) {
        this.processor = processor;
    }

    abstract void readFile(String path);
}
