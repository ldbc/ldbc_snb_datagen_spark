package ldbc.snb.datagen.generator.serializers

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.{GenActivity, PersonActivityGenerator, SparkRanker}
import ldbc.snb.datagen.generator.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.io.Writer
import ldbc.snb.datagen.io.raw.csv.CsvRowEncoder
import ldbc.snb.datagen.io.raw.parquet.ParquetRowEncoder
import ldbc.snb.datagen.io.raw.{RawSink, WriteContext, recordOutputStream}
import ldbc.snb.datagen.model.raw._
import ldbc.snb.datagen.model.{EntityTraits, raw}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{Job, JobID, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.text.SimpleDateFormat
import java.util
import java.util.{Collections, Date, Locale, UUID}
import java.util.function.Consumer

class RawSerializer(ranker: SparkRanker)(implicit spark: SparkSession) extends Writer[RawSink] with Logging {
  override type Data = RDD[Person]

  private def writeDynamicSubgraph(persons: RDD[Person], sink: RawSink): Unit = {

    val blockSize = DatagenParams.blockSize
    val blocks = ranker(persons)
      .map { case (k, v) => (k / blockSize, v) }
      .groupByKey()

    val job = RawSerializationJobContext(persons.sparkContext.hadoopConfiguration, sink)

    job.run(blocks)((groups, wc) => {
      DatagenContext.initialize(sink.conf)

      def stream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder] =
        recordOutputStream(sink, wc)

      import ldbc.snb.datagen.io.raw.instances._
      import ldbc.snb.datagen.model.raw.instances._
      import ldbc.snb.datagen.util.sql._

      val pos = new PersonOutputStream(
        stream[raw.Person],
        stream[raw.PersonKnowsPerson],
        stream[raw.PersonHasInterestTag],
        stream[raw.PersonStudyAtUniversity],
        stream[raw.PersonWorkAtCompany]
      )

      val activityStream = new ActivityOutputStream(
        stream[Forum],
        stream[ForumHasTag],
        stream[ForumHasMember],
        stream[Post],
        stream[PostHasTag],
        stream[Comment],
        stream[CommentHasTag],
        stream[PersonLikesPost],
        stream[PersonLikesComment]
      )

      val generator = new PersonActivityGenerator

      (activityStream, pos) use { case (activityStream, pos) =>
        for { (blockId, persons) <- groups } {
          val personList = new util.ArrayList[Person](persons.size)
          for (p <- persons) {
            personList.add(p)
          }
          Collections.sort(personList)

          personList.forEach(new Consumer[Person] {
            override def accept(t: Person): Unit = pos.write(t)
          })

          val activities = generator.generateActivityForBlock(blockId.toInt, personList)

          activities.forEach(new Consumer[GenActivity] {
            override def accept(t: GenActivity): Unit = activityStream.write(t)
          })
        }
      }
    })
  }

  private def writeStaticSubgraph(persons: RDD[Person], sink: RawSink): Unit = {
    val job = RawSerializationJobContext(persons.sparkContext.hadoopConfiguration, sink)
    // we need to do this in an executor to get a TaskContext
    job.run(persons.sparkContext.parallelize(Seq(0), 1))((_, wc) => {
      DatagenContext.initialize(sink.conf)

      def stream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder] =
        recordOutputStream(sink, wc)

      import ldbc.snb.datagen.io.raw.instances._
      import ldbc.snb.datagen.model.raw.instances._
      import ldbc.snb.datagen.util.sql._

      val staticStream = new StaticOutputStream(
        stream[Place],
        stream[Tag],
        stream[TagClass],
        stream[Organisation]
      )

      staticStream use {
        _.write(StaticGraph)
      }
    })
  }

  override def write(self: RDD[Person], sink: RawSink): Unit = {
    writeDynamicSubgraph(self, sink)
    writeStaticSubgraph(self, sink)
  }
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
    val jobId         = createJobID(new Date(jobIdInstant), tc.stageId())
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

  private[this] def createJobID(time: Date, id: Int): JobID = {
    val jobtrackerID = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(time)
    new JobID(jobtrackerID, id)
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
      case e =>
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
      throw new AssertionError("Raw output already exists.")
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
