package blog.iamrob

import scopt.OptionParser

case class UsageConfig(inputPath: String = "", outputPath: String = "")

class UsageOptionParser
  extends OptionParser[UsageConfig]("job config") {
  head("scopt", "3.x")

  opt[String]('d', "inputPath").required
    .action((value, arg) => {
      arg.copy(inputPath = value)
    })
    .text("Path to the taxi trip data csv file")

  opt[String]('d', "outputPath") // not required, at least for now
    .action((value, arg) => {
      arg.copy(outputPath = value)
    })
    .text("Path to the output file")
}
