/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.thirdparty.parquet;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageWriteStore;
import parquet.column.page.PageWriter;
import parquet.column.statistics.BooleanStatistics;
import parquet.column.statistics.Statistics;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.io.ParquetEncodingException;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.storage.thirdparty.parquet.CodecFactory.BytesCompressor;
import static parquet.Log.DEBUG;

class ColumnChunkPageWriteStore implements PageWriteStore {
  private static final Log LOG = Log.getLog(ColumnChunkPageWriteStore.class);

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static final class ColumnChunkPageWriter implements PageWriter {

    private final ColumnDescriptor path;
    private final BytesCompressor compressor;

    private final CapacityByteArrayOutputStream buf;
    private DictionaryPage dictionaryPage;

    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;
    private int pageCount;

    private Set<Encoding> encodings = new HashSet<>();

    private Statistics totalStatistics;

    private ColumnChunkPageWriter(ColumnDescriptor path, BytesCompressor compressor, int initialSize) {
      this.path = path;
      this.compressor = compressor;
      this.buf = new CapacityByteArrayOutputStream(initialSize);
      this.totalStatistics = Statistics.getStatsBasedOnType(this.path.getType());
    }

    @Deprecated
    @Override
    public void writePage(BytesInput bytes,
                          int valueCount,
                          Encoding rlEncoding,
                          Encoding dlEncoding,
                          Encoding valuesEncoding) throws IOException {
      long uncompressedSize = bytes.size();
      BytesInput compressedBytes = compressor.compress(bytes);
      long compressedSize = compressedBytes.size();
      BooleanStatistics statistics = new BooleanStatistics(); // dummy stats object
      parquetMetadataConverter.writeDataPageHeader(
          (int)uncompressedSize,
          (int)compressedSize,
          valueCount,
          statistics,
          rlEncoding,
          dlEncoding,
          valuesEncoding,
          buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      compressedBytes.writeAllTo(buf);
      encodings.add(rlEncoding);
      encodings.add(dlEncoding);
      encodings.add(valuesEncoding);
    }

    @Override
    public void writePage(BytesInput bytes,
                          int valueCount,
                          Statistics statistics,
                          Encoding rlEncoding,
                          Encoding dlEncoding,
                          Encoding valuesEncoding) throws IOException {
      long uncompressedSize = bytes.size();
      BytesInput compressedBytes = compressor.compress(bytes);
      long compressedSize = compressedBytes.size();
      parquetMetadataConverter.writeDataPageHeader(
          (int)uncompressedSize,
          (int)compressedSize,
          valueCount,
          statistics,
          rlEncoding,
          dlEncoding,
          valuesEncoding,
          buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(statistics);
      compressedBytes.writeAllTo(buf);
      encodings.add(rlEncoding);
      encodings.add(dlEncoding);
      encodings.add(valuesEncoding);
    }

    @Override
    public long getMemSize() {
      return buf.size();
    }

    public void writeToFileWriter(ParquetFileWriter writer) throws IOException {
      writer.startColumn(path, totalValueCount, compressor.getCodecName());
      if (dictionaryPage != null) {
        writer.writeDictionaryPage(dictionaryPage);
        encodings.add(dictionaryPage.getEncoding());
      }
      writer.writeDataPages(BytesInput.from(buf), uncompressedLength, compressedLength, totalStatistics, new ArrayList<>(encodings));
      writer.endColumn();
      if (DEBUG) {
        LOG.debug(
            String.format(
                "written %,dB for %s: %,d values, %,dB raw, %,dB comp, %d pages, encodings: %s",
                buf.size(), path, totalValueCount, uncompressedLength, compressedLength, pageCount, encodings)
                + (dictionaryPage != null ? String.format(
                ", dic { %,d entries, %,dB raw, %,dB comp}",
                dictionaryPage.getDictionarySize(), dictionaryPage.getUncompressedSize(), dictionaryPage.getDictionarySize())
                : ""));
      }
      encodings.clear();
      pageCount = 0;
    }

    @Override
    public long allocatedSize() {
      return buf.getCapacity();
    }

    @Override
    public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
      if (this.dictionaryPage != null) {
        throw new ParquetEncodingException("Only one dictionary page is allowed");
      }
      BytesInput dictionaryBytes = dictionaryPage.getBytes();
      int uncompressedSize = (int)dictionaryBytes.size();
      BytesInput compressedBytes = compressor.compress(dictionaryBytes);
      this.dictionaryPage = new DictionaryPage(BytesInput.copy(compressedBytes), uncompressedSize, dictionaryPage.getDictionarySize(), dictionaryPage.getEncoding());
    }

    @Override
    public String memUsageString(String prefix) {
      return buf.memUsageString(prefix + " ColumnChunkPageWriter");
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageWriter> writers = new HashMap<>();
  private final MessageType schema;
  private final BytesCompressor compressor;
  private final int initialSize;

  public ColumnChunkPageWriteStore(BytesCompressor compressor, MessageType schema, int initialSize) {
    this.compressor = compressor;
    this.schema = schema;
    this.initialSize = initialSize;
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    if (!writers.containsKey(path)) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor, initialSize));
    }
    return writers.get(path);
  }

  public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
    List<ColumnDescriptor> columns = schema.getColumns();
    for (ColumnDescriptor columnDescriptor : columns) {
      ColumnChunkPageWriter pageWriter = writers.get(columnDescriptor);
      pageWriter.writeToFileWriter(writer);
    }
  }

}