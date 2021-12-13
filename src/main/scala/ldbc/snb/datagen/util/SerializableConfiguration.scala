package ldbc.snb.datagen.util

import org.apache.hadoop.conf.Configuration

import java.io.{ObjectInputStream, ObjectOutputStream}

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = tryOrThrowIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrThrowIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}
