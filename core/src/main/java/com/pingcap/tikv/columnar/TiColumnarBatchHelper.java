package com.pingcap.tikv.columnar;

import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A helper class to create {@link ColumnarBatch} from {@link TiColumnarChunk}
 */
public final class TiColumnarBatchHelper {
  public static ColumnarBatch createColumnarBatch(TiColumnarChunk chunk) {
    TiColumnVectorAdapter[] columns = new TiColumnVectorAdapter[chunk.numOfCols()];
    for(int i = 0; i < chunk.numOfCols(); i++) {
      columns[i] = new TiColumnVectorAdapter(chunk.column(i));
    }
    ColumnarBatch batch = new ColumnarBatch(columns);
    batch.setNumRows(chunk.numOfRows());
    return batch;
  }
}
