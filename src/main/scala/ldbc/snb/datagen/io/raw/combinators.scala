package ldbc.snb.datagen.io.raw

object combinators {

  trait MakeBatchPart[T] {
    def apply(part: Int): RecordOutputStream[T]
    def exists(): Boolean
    def delete(): Unit
  }

  final class FixedSizeBatchOutputStream[T](size: Long, makeBatchPart: MakeBatchPart[T]) extends RecordOutputStream[T] {
    private var written: Long           = 0
    private var currentBatchNumber: Int = 0
    private var currentBatch            = makeBatchPart(currentBatchNumber)

    override def write(t: T): Unit = {
      if (currentBatch == null)
        throw new AssertionError("Stream already closed")

      if (written >= size) {
        currentBatch.close()
        currentBatchNumber = currentBatchNumber + 1
        currentBatch = makeBatchPart(currentBatchNumber)
        written = 0
      }

      currentBatch.write(t)
      written = written + 1
    }

    override def close(): Unit = {
      if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }
    }
  }

  final class RoundRobinOutputStream[T](outputStreams: Seq[RecordOutputStream[T]]) extends RecordOutputStream[T] {
    private var current = 0
    private val n       = outputStreams.length

    override def write(t: T): Unit = {
      outputStreams(current).write(t)
      current = (current + 1) % n
    }

    override def close(): Unit = for { os <- outputStreams } os.close()
  }
}
