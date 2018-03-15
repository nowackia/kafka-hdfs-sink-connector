package com.github.nowackia.kafka.connect.hdfs.writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Writer for CSV files
 */
public class CsvFileWriter extends AbstractFileWriter {

    private static final Logger log = LoggerFactory.getLogger(CsvFileWriter.class);

    public CsvFileWriter(FileSystem fs, String location, String fileName, boolean isFirst, long total, Schema schema, boolean compressed) throws IOException {
        super(fs, location, fileName, isFirst, isFirst, total, schema, compressed);
    }

    public void write(ArrayList<Struct> structs) {
        for (Struct struct : structs) {
            try {
                StringBuilder row = new StringBuilder();
                for (int i = 0; i < schema.fields().size(); i++) {
                    row.append(struct.get(schema.fields().get(i)));
                    if (i < schema.fields().size() - 1)
                        row.append(",");
                    else
                        row.append(System.lineSeparator());
                }
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