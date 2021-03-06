package com.github.nowackia.kafka.connect.hdfs.writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Writer for text files (including json files)
 */
public class TextFileWriter extends AbstractFileWriter {

    private static final Logger log = LoggerFactory.getLogger(TextFileWriter.class);

    public TextFileWriter(FileSystem fs, String location, String fileName, boolean isFirst, long total, Schema schema, boolean compressed) throws IOException {
        super(fs, location, fileName, isFirst, false, total, schema, compressed);
    }

    public void write(ArrayList<Struct> structs) {
        for (Struct struct : structs) {
            try {
                StringBuilder row = new StringBuilder();
                row.append(struct.get(schema.fields().get(0)).toString());
                row.append(System.lineSeparator());

                if (out != null) {
                    out.write(row.toString().getBytes());
                    dataWritten += 1;
                }
            } catch (Exception e) {
                log.error("Error writing to output stream", e);
            }
        }
    }
}