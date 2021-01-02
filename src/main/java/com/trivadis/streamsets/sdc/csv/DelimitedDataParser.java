package com.trivadis.streamsets.sdc.csv;

import com.streamsets.pipeline.api.impl.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;

public interface DelimitedDataParser extends Closeable {
    default long skipLines(Reader reader, int lines) throws IOException {
        int count = 0;
        int skipped = 0;
        while (skipped < lines) {
            int c = reader.read();
            if (c == -1) {
                throw new IOException(Utils.format("Could not skip '{}' lines, reached EOF", lines));
            }
            // this is enough to handle \n and \r\n EOL files
            if (c == '\n') {
                skipped++;
            }
            count++;
        }
        return count;
    }

    String[] getHeaders() throws IOException;
    String[] read() throws IOException;
    long getReaderPosition();
    void close() throws IOException;
}