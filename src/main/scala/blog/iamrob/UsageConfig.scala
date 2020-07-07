package blog.iamrob

import scopt.OptionParser

case class UsageConfig(
  inputPath: String = "",
  inputFormat: String = "",
  inputYear: String = "",
  inputMonth: String = "",
  outputPath: String = "",
  outputFormat: String = "",
  outputMode: String = "")

class UsageOptionParser
  extends OptionParser[UsageConfig]("job config") {
  head("scopt", "3.x")

  opt[String]('d', "inputPath").required
    .action((value, arg) => {
      arg.copy(inputPath = value)
    })
    .text("Path to the taxi trip data csv file")

  opt[String]('d', "inputFormat").required
    .action((value, arg) => {
      arg.copy(inputFormat = value)
    })
    .validate(x =>
      if (Array("orc", "parquet", "avro", "csv").contains(x)) success
      else failure(f"Invalid input format '$x'")
    )
    .text("Input format: orc, parquet, avro, csv")

  opt[String]('d', "inputYear")
    .action((value, arg) => {
      arg.copy(inputYear = value)
    })
    .text("Year of the dataset")

  opt[String]('d', "inputMonth")
    .action((value, arg) => {
      arg.copy(inputMonth = value)
    })
    .text("Month of the dataset")

  opt[String]('d', "outputPath")
    .action((value, arg) => {
      arg.copy(outputPath = value)
    })
    .text("Path to the output file")

  opt[String]('d', "outputFormat")
    .action((value, arg) => {
      arg.copy(outputFormat = value)
    })
    .validate(x =>
      if (Array("orc", "parquet", "avro").contains(x)) success
      else failure(f"Invalid output format '$x'")
    )
    .text("Output format: orc, parquet, avro")

  opt[String]('d', "outputMode")
    .action((value, arg) => {
      arg.copy(outputMode = value)
    })
    .validate(x =>
      if (Array("overwrite", "append").contains(x)) success
      else failure(f"Invalid output mode '$x'")
    )
    .text("Output mode: overwrite, append")

}
