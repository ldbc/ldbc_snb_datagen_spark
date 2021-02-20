package ldbc.snb.datagen.transformation.io

import ldbc.snb.datagen.transformation.model.Mode
import ldbc.snb.datagen.transformation.model.Mode.Raw

class FormatOptions(val format: String, mode: Mode, private val customFormatOptions: Map[String, String] = Map.empty) {
  val defaultCsvFormatOptions = Map(
    "header" -> "true",
    "sep" ->  "|"
  )

  val forcedRawCsvFormatOptions = Map(
    "dateFormat" -> Raw.datePattern,
    "timestampFormat" -> Raw.dateTimePattern
  )

  val formatOptions: Map[String, String] = (format, mode) match {
    case ("csv", Raw) => defaultCsvFormatOptions ++ customFormatOptions ++ forcedRawCsvFormatOptions
    case ("csv", _) => defaultCsvFormatOptions ++ customFormatOptions
    case _ => customFormatOptions
  }
}
