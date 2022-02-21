package ldbc.snb.datagen.io

import ldbc.snb.datagen.io.raw.combinators.FixedSizeBatchOutputStream
import ldbc.snb.datagen.io.raw.csv.{CsvRowEncoder, MakeCsvBatchPart}
import ldbc.snb.datagen.io.raw.parquet.{MakeParquetBatchPart, ParquetRowEncoder}
import ldbc.snb.datagen.model.EntityTraits
import ldbc.snb.datagen.util.{GeneratorConfiguration, Logging}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}

package object raw {
  trait RecordOutputStream[T] extends AutoCloseable {
    def write(t: T)
  }

  sealed trait RawFormat
  case object Csv     extends RawFormat { override def toString = "csv"     }
  case object Parquet extends RawFormat { override def toString = "parquet" }

  case class RawSink(
      format: RawFormat,
      partitions: Option[Int] = None,
      conf: GeneratorConfiguration,
      oversizeFactor: Option[Double] = None,
      overwrite: Boolean = false
  )

  class WriteContext(
      val taskContext: TaskContext,
      val taskAttemptContext: TaskAttemptContext,
      val hadoopConf: Configuration,
      val fileSystem: FileSystem
  )

  // See org.apache.parquet.hadoop.metadata.CompressionCodecName for other options. Note that not all are
  // supported by Spark.
  val DefaultParquetCompression = "snappy"

  val DefaultBatchSize: Long = 10000000

  def recordOutputStream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder](sink: RawSink, writeContext: WriteContext): RecordOutputStream[T] = {
    val et                           = EntityTraits[T]
    val size                         = (DefaultBatchSize * sink.oversizeFactor.getOrElse(1.0)).toLong
    implicit val encoder: Encoder[T] = ParquetRowEncoder[T].encoder
    val entityPath                   = et.`type`.entityPath
    val pathPrefix                   = s"${sink.conf.getOutputDir}/graphs/${sink.format.toString}/raw/composite-merged-fk/${entityPath}"

    val makeBatchPart = sink.format match {
      case Parquet => new MakeParquetBatchPart[T](pathPrefix, writeContext)
      case Csv     => new MakeCsvBatchPart[T](pathPrefix, writeContext)
      case x       => throw new UnsupportedOperationException(s"Raw serializer not implemented for format ${x}")
    }
    makeBatchPart.delete()
    new FixedSizeBatchOutputStream[T](size, makeBatchPart)
  }

  class RawSerializationTaskContext(
      taskContext: TaskContext,
      committer: FileCommitProtocol,
      configuration: SerializableConfiguration,
      sink: RawSink,
      outputPath: String
  ) extends Logging
      with Serializable {

    private[this] def createNewWriteContext(taskContext: TaskContext, hadoopConf: Configuration, fs: FileSystem) = {
      val jobIdInstant = new Date().getTime
      new WriteContext(taskContext, getTaskAttemptContext(hadoopConf, TaskContext.get, jobIdInstant), hadoopConf, fs)
    }

    private[this] def getTaskAttemptContext(conf: Configuration, tc: TaskContext, jobIdInstant: Long): TaskAttemptContext = {
      val jobtrackerID  = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date(jobIdInstant))
      val jobId         = new JobID(jobtrackerID, tc.stageId())
      val taskId        = new TaskID(jobId, TaskType.MAP, tc.partitionId())
      val taskAttemptId = new TaskAttemptID(taskId, tc.taskAttemptId().toInt & Integer.MAX_VALUE)

      // Set up the attempt context required to use in the output committer.
      {
        // Set up the configuration object
        conf.set("mapreduce.job.id", jobId.toString)
        conf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
        conf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
        conf.setBoolean("mapreduce.task.ismap", true)
        conf.setInt("mapreduce.task.partition", 0)
        new TaskAttemptContextImpl(conf, taskAttemptId)
      }
    }

    def runTask(task: (WriteContext => Unit)) = {
      val conf     = configuration.value
      val buildDir = sink.conf.getOutputDir
      val fs       = FileSystem.get(new URI(buildDir), conf)
      val ctx      = createNewWriteContext(taskContext, conf, fs)
      fs.mkdirs(new Path(outputPath))
      committer.setupTask(ctx.taskAttemptContext)

      try {
        tryWithSafeFinallyAndFailureCallbacks(block = {
          task(ctx)
          committer.commitTask(ctx.taskAttemptContext)
        })(
          catchBlock = {
            committer.abortTask(ctx.taskAttemptContext)
          },
          finallyBlock = {}
        )
      } catch {
        case e: Throwable =>
          throw e
        //      case e: FetchFailedException =>
        //        throw e
        //      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        //        // If any output file to write already exists, it does not make sense to re-run this task.
        //        // We throw the exception and let Executor throw ExceptionFailure to abort the job.
        //        throw new TaskOutputFileAlreadyExistException(f)
        //      case t: Throwable =>
        //        throw QueryExecutionErrors.taskFailedWhileWritingRowsError(t)
      }

    }

    def tryWithSafeFinallyAndFailureCallbacks[T](block: => T)(catchBlock: => Unit = (), finallyBlock: => Unit = ()): T = {
      var originalThrowable: Throwable = null
      try {
        block
      } catch {
        case cause: Throwable =>
          // Purposefully not using NonFatal, because even fatal exceptions
          // we don't want to have our finallyBlock suppress
          originalThrowable = cause
          try {
            log.error("Aborting task", originalThrowable)
            if (TaskContext.get() != null) {
              // TaskContext.get().markTaskFailed(originalThrowable)
            }
            catchBlock
          } catch {
            case t: Throwable =>
              if (originalThrowable != t) {
                originalThrowable.addSuppressed(t)
                log.warn(s"Suppressing exception in catch: ${t.getMessage}", t)
              }
          }
          throw originalThrowable
      } finally {
        try {
          finallyBlock
        } catch {
          case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
            originalThrowable.addSuppressed(t)
            log.warn(s"Suppressing exception in finally: ${t.getMessage}", t)
            throw originalThrowable
        }
      }
    }
  }

  class RawSerializationJobContext(
      @transient job: Job, // not serializable
      committer: FileCommitProtocol,
      configuration: SerializableConfiguration,
      sink: RawSink,
      outputPath: String
  ) extends Serializable {

    def run[T](rdd: RDD[T])(exec: (Iterator[T], WriteContext) => Unit)(implicit spark: SparkSession) = {
      val fs     = FileSystem.get(URI.create(outputPath), configuration.value)
      val path   = new Path(outputPath)
      val exists = fs.exists(path)

      if (exists && !sink.overwrite) {
        throw new AssertionError(s"Directory already exists: ${outputPath}")
      } else if (exists) {
        fs.delete(path, true)
      }

      committer.setupJob(job)
      try {
        val ret = new Array[TaskCommitMessage](rdd.partitions.length)
        spark.sparkContext.runJob[T, TaskCommitMessage](
          rdd,
          (taskContext: TaskContext, iter: Iterator[T]) => {
            val ctx = new RawSerializationTaskContext(taskContext, committer, configuration, sink, outputPath)
            ctx.runTask { wc => exec(iter, wc) }
          },
          rdd.partitions.indices,
          (index: Int, res: TaskCommitMessage) => {
            committer.onTaskCommit(res)
            ret(index) = res
          }
        )
        committer.commitJob(job, ret)
      } catch {
        case t: Throwable =>
          committer.abortJob(job)
          throw t
      }
    }
  }

  object RawSerializationJobContext {
    def apply(conf: Configuration, sink: RawSink)(implicit spark: SparkSession) = {
      val job = Job.getInstance(conf)
      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(classOf[InternalRow])

      val outputPath = s"${sink.conf.getOutputDir}/graphs/${sink.format.toString}/raw/composite-merged-fk"

      val jobId = UUID.randomUUID().toString

      val committer = FileCommitProtocol.instantiate(
        spark.sessionState.conf.fileCommitProtocolClass,
        jobId = jobId,
        outputPath = outputPath
      )
      job.getConfiguration.set("spark.sql.sources.writeJobUUID", jobId)
      FileOutputFormat.setOutputPath(job, new Path(outputPath))
      new RawSerializationJobContext(job, committer, new SerializableConfiguration(conf), sink, outputPath)
    }
  }

  object instances extends csv.CsvRowEncoderInstances with parquet.ParquetRowEncoderInstances
}
