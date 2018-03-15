package com.github.nowackia.kafka.connect.hdfs.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

/**
 * Class used for writing out the data to source (both file system and hdfs)
 */
public abstract class AbstractFileWriter implements FileWriter {
    private static final Logger log = LoggerFactory.getLogger(AbstractFileWriter.class);

    protected final String fileName;
    protected final OutputStream out;
    protected final Schema schema;

    protected short percentageRead = 0;
    protected final long total;
    protected long dataWritten;

    public AbstractFileWriter(FileSystem fs, String location, String fileName, boolean isFirst, boolean writeHeader, long total, Schema schema, boolean compressed) throws IOException {
        Path path = new Path(location);

        if(!fs.exists(path)) {
            log.info("Directory {} does not exist, creating it now", path);
            fs.mkdirs(path);
        }
        fs.setWorkingDirectory(path);

        this.dataWritten = 0;
        this.total = total;
        this.schema = schema;

        if(compressed)
            this.fileName = fileName + ".gz";
        else
            this.fileName = fileName;

        FSDataOutputStream stream = createStream(fs, new Path(this.fileName), isFirst);
        if(compressed) {
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
            CompressionCodec codec = codecFactory.getCodecByName("GzipCodec");
            this.out = codec.createOutputStream(stream);
        } else {
            this.out = stream;
        }

        if(writeHeader) {
            StringBuilder header = new StringBuilder();
            for (int i = 0; i < schema.fields().size(); i++) {
                header.append(schema.fields().get(i).name());
                if (i < schema.fields().size() - 1)
                    header.append(",");
                else
                    header.append(System.lineSeparator());
            }
            out.write(header.toString().getBytes());
        }
    }

    private FSDataOutputStream createStream(FileSystem fs, Path filePath, boolean overwrite) throws IOException {
        FSDataOutputStream stream = null;

        int bufferSize = 4096;
        short replication = fs.getDefaultReplication(filePath);;
        long blockSize = fs.getDefaultBlockSize(filePath);

        if(overwrite)
            stream = fs.create(filePath, null, true, bufferSize, replication, blockSize, null);
        else {
            if(fs.exists(filePath))
                stream = fs.append(filePath);
            else
                stream = fs.create(filePath, null, false, bufferSize, replication, blockSize, null);
        }

        return stream;
    }

    @Override
    public abstract void write(ArrayList<Struct> structs);

    @Override
    public void close() {
        try {
            if (out != null)
                out.close();
        } catch(IOException e) {
            log.error("Error flushing stream", e);
        }
    }

    @Override
    public void flush() {
        try {
            if(out != null)
                out.flush();
        } catch(IOException e) {
            log.error("Error flushing stream", e);
        }
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public void logProgress(Logger log, int interval) {
        long written = Math.max(dataWritten - 1, 0);

        short value = percentage(written, total, interval);
        if (value != percentageRead) {
            percentageRead = value;
            log.info("File " + fileName + " written in over " + percentageRead + "% [" + written + " from " + total + "]");
        }
    }

    private short percentage(long value, long max, int interval) {
        int percentage = (int)((((double)value / (double)max) * 100.0) / interval) * interval;
        return (short)Math.min(percentage, 100);
    };
}