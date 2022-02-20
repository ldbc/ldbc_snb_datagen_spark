package ldbc.snb.datagen.util

import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  @transient lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
}
