package com.github.nowackia.kafka.connect.hdfs.writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Writer for binary files
 */
public class BinaryFileWriter extends AbstractFileWriter  {

    private static final Logger log = LoggerFactory.getLogger(TextFileWriter.class);

    public BinaryFileWriter(FileSystem fs, String location, String fileName, boolean isFirst, long total, Schema schema, boolean compressed) throws IOException {
        super(fs, location, fileName, isFirst, false, total, schema, compressed);
    }

    @Override
    public void write(ArrayList<Struct> structs) {
        for (Struct struct : structs) {
            try {
                byte[] data = (byte[])struct.get(schema.fields().get(0));
                int len = (int)struct.get(schema.fields().get(1));

                if (out != null) {
                    out.write(data, 0, len);
                    dataWritten += len;
                }

            } catch (Exception e) {
                log.error("Error writing to output stream", e);
            }
        }
    }
}