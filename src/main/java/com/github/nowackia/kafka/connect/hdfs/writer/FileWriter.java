package com.github.nowackia.kafka.connect.hdfs.writer;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * Interface for file writer
 */
public interface FileWriter {

    void write(ArrayList<Struct> structs);

    void close();

    void flush();

    String getFileName();

    void logProgress(Logger log, int interval);

}
