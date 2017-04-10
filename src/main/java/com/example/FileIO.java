package com.example;
import java.io.ByteArrayOutputStream;
import com.google.cloud.dataflow.sdk.io.*;
import java.io.IOException;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.avro.Schema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import java.nio.ByteBuffer;
import com.google.cloud.dataflow.sdk.coders.*;
import java.util.NoSuchElementException;
import java.nio.channels.*;
import org.slf4j.LoggerFactory;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.io.File;
public class FileIO {
// Match TextIO.
public static Read.Bounded<KV<String,String>> readFilepattern(String filepattern) {
    return Read.from(new FileSource(filepattern, 1));
}

public static class FileSource extends FileBasedSource<KV<String,String>> {
    private String filename = null;

    public FileSource(String fileOrPattern, long minBundleSize) {
        super(fileOrPattern, minBundleSize);
    }

    public FileSource(String filename, long minBundleSize, long startOffset, long endOffset) {
        super(filename, minBundleSize, startOffset, endOffset);
        this.filename = filename;
    }

    // This will indicate that there is no intra-file splitting.
    @Override
    public boolean isSplittable(){
        return false;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return false;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<KV<String,String>> getDefaultOutputCoder() {
        return KvCoder.of(StringUtf8Coder.of(),StringUtf8Coder.of());
    }

    @Override
    public FileBasedSource<KV<String,String>> createForSubrangeOfFile(String fileName, long start, long end) {
        return new FileSource(fileName, getMinBundleSize(), start, end);
    }

    @Override
    public FileBasedReader<KV<String,String>> createSingleFileReader(PipelineOptions options) {
        return new FileReader(this);
    }
}

/**
 * A reader that should read entire file of text from a {@link FileSource}.
 */
private static class FileReader extends FileBasedSource.FileBasedReader<KV<String,String>> {
    private static final Logger LOG = LoggerFactory.getLogger(FileReader.class);
    private ReadableByteChannel channel = null;
    private long nextOffset = 0;
    private long currentOffset = 0;
    private boolean isAtSplitPoint = false;
    private final ByteBuffer buf;
    private static final int BUF_SIZE = 1024;
    private KV<String,String> currentValue = null;
    private String filename;

    public FileReader(FileSource source) {
        super(source);
        buf = ByteBuffer.allocate(BUF_SIZE);
        buf.flip();
        this.filename = source.filename;
    }

    private int readFile(ByteArrayOutputStream out) throws IOException {
        int byteCount = 0;
        while (true) {
            if (!buf.hasRemaining()) {
                buf.clear();
                int read = channel.read(buf);
                if (read < 0) {
                    break;
                }
                buf.flip();
            }
            byte b = buf.get();
            byteCount++;

            out.write(b);
        }
        return byteCount;
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
        this.channel = channel;
    }

    @Override
    protected boolean readNextRecord() throws IOException {
        currentOffset = nextOffset;

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int offsetAdjustment = readFile(buf);
        if (offsetAdjustment == 0) {
            // EOF
            return false;
        }
        nextOffset += offsetAdjustment;
        isAtSplitPoint = true;
        currentValue = KV.of(this.filename,CoderUtils.decodeFromByteArray(StringUtf8Coder.of(), buf.toByteArray()));
        return true;
    }

    @Override
    protected boolean isAtSplitPoint() {
        return isAtSplitPoint;
    }

    @Override
    protected long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public KV<String,String> getCurrent() throws NoSuchElementException {
        return currentValue;
    }
}

}
