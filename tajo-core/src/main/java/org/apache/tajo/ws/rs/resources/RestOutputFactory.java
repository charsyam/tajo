package org.apache.tajo.ws.rs.resources;

import com.google.protobuf.ByteString;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.storage.Tuple;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by charsyam on 2015. 11. 10..
 */
public class RestOutputFactory {
    public static abstract class RestStreamingOutput implements StreamingOutput {
        private final NonForwardQueryResultScanner cachedQueryResultScanner;
        private final int rowNum;
        private int count;

        public RestStreamingOutput(NonForwardQueryResultScanner cachedQueryResultScanner, int rowNum) throws IOException {
            this.cachedQueryResultScanner = cachedQueryResultScanner;
            this.rowNum = rowNum;
        }

        public abstract int length();

        public abstract boolean hasLength();

        public void setCount(int count) {
            this.count = count;
        }

        public int count() {
            return this.count;
        }
    }

    public static RestStreamingOutput getStreamingOutput(String type, NonForwardQueryResultScanner cachedQueryResultScanner, int rowNum) throws IOException{
        if (type.equalsIgnoreCase("csv")) {
            return new CSVQueryResultStreamingOutput(cachedQueryResultScanner, rowNum);
        } else {
            return new BinaryQueryResultStreamingOutput(cachedQueryResultScanner, rowNum);
        }
    }

    private static class BinaryQueryResultStreamingOutput extends RestStreamingOutput {
        private final List<ByteString> outputList;

        public BinaryQueryResultStreamingOutput(NonForwardQueryResultScanner cachedQueryResultScanner, int rowNum) throws IOException {
            super(cachedQueryResultScanner, rowNum);
            this.outputList = cachedQueryResultScanner.getNextRows(rowNum);
            setCount(outputList.size());
        }

        @Override
        public boolean hasLength() {
            return false;
        }

        @Override
        public int length() {
            return -1;
        }

        @Override
        public void write(OutputStream outputStream) throws IOException, WebApplicationException {
            DataOutputStream streamingOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream));

            for (ByteString byteString : outputList) {
                byte[] byteStringArray = byteString.toByteArray();
                streamingOutputStream.writeInt(byteStringArray.length);
                streamingOutputStream.write(byteStringArray);
            }

            streamingOutputStream.flush();
        }
    }

    private static class CSVQueryResultStreamingOutput extends RestStreamingOutput {
        private final List<Tuple> outputTuples;
        private String output = null;
        private boolean isLoaded = false;
        private int outputSize = 0;
        private Schema schema;
        private int startOffset;

        public CSVQueryResultStreamingOutput(NonForwardQueryResultScanner cachedQueryResultScanner, int rowNum) throws IOException {
            super(cachedQueryResultScanner, rowNum);
            this.startOffset = cachedQueryResultScanner.getCurrentRowNumber();
            this.outputTuples = cachedQueryResultScanner.getNextTupleRows(rowNum);
            this.schema = cachedQueryResultScanner.getLogicalSchema();
            setCount(outputTuples.size());

        }

        @Override
        public boolean hasLength() {
            return true;
        }

        private int calulateLength() {
            StringBuilder sb = new StringBuilder();
            if (startOffset == 0) {
                List<Column> columns = schema.getAllColumns();

                int columnSize = columns.size();
                for (int i = 0; i < columnSize; i++) {
                    if (i > 0) {
                        sb.append(",");
                    }

                    Column column = columns.get(i);
                    sb.append(StringEscapeUtils.escapeCsv(column.getSimpleName()));
                }

                sb.append("\r\n");
            }

            for (Tuple tuple : outputTuples) {
                Datum [] datums = tuple.getValues();
                int size = datums.length;

                for (int i = 0; i < size; i++) {
                    if (i > 0) {
                        sb.append(",");
                    }

                    Datum datum = datums[i];
                    String s = StringEscapeUtils.escapeCsv(datum.toString());
                    sb.append(s);
                }
                sb.append("\r\n");
            }

            output = sb.toString();
            isLoaded = true;
            return output.length();
        }

        @Override
        public int length() {
            if (isLoaded == false) {
                calulateLength();
            }

            return output.length();
        }

        @Override
        public void write(OutputStream outputStream) throws IOException, WebApplicationException {
            if (isLoaded == false) {
                calulateLength();
            }

            DataOutputStream streamingOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream));
            streamingOutputStream.write(output.getBytes("UTF-8"));
            streamingOutputStream.flush();
        }
    }
}

