import java.io.{File, IOException}
import java.nio.file.{FileSystems, Files, Path, Paths}
import java.util
import java.util.stream.Collectors

import com.beust.jcommander.JCommander
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object TeamSchlau {
  def main(args: Array[String]): Unit = {
    // Parse input arguments
    /*val arguments = new Args();
    JCommander
      .newBuilder
      .addObject(arguments)
      .build
      .parse(args)

    val datasetPath = FileSystems
      .getDefault
      .getPath(arguments.getDatasetPath)
      .toAbsolutePath
      .toString

    val cores = arguments.getCores*/
    val cores = 4
    val datasetPath = "./data/testdata"

    // Get all .csv-files for the dataset path
    val fileRegex = """.*\.csv$""".r
    val d = new File(datasetPath)
    val files = d.listFiles
      .filter(_.isFile)
      .filter(file => fileRegex.findFirstIn(file.getName()).isDefined)
      .toList

    // Configure Spark session
    val sparkSess: SparkSession = SparkSession
      .builder()
      .appName("Team Schlau")
      .master(s"local[$cores]") // local, with <cores> worker cores
      .getOrCreate()

    sparkSess.conf.set("spark.sql.shuffle.partitions", "16")

    // Read .csv-files
    val datasets: Seq[DataFrame] = files.map(file => {
      sparkSess.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(file.getAbsolutePath)
    })

    print("\nSTART\n\n")
    datasets(0).show()

    // Find inclusion dependencies
    // TODO: Find inclusion dependencies
  }
}
