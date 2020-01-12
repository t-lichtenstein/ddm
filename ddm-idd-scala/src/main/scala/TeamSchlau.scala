import java.io.{File}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object TeamSchlau {

  def createFromStringCombiner = (value: (String)) => Set(value)
  def createFromSetCombiner = (value: (Set[String])) => value

  def mergeStringsToSet = (accumulator: Set[String], element: String) =>
    (accumulator + element)

  def mergeSets = (accumulator1: (Set[String]), accumulator2: (Set[String])) =>
    (accumulator1.union(accumulator2))


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
    val cores = 1
    val datasetPath = "./data/TPCH"

    // Get all .csv-files for the dataset path
    val fileRegex = """.*\.csv$""".r
    val d = new File(datasetPath)
    val files = d.listFiles
      .filter(_.isFile)
      .filter(file => fileRegex.findFirstIn(file.getName()).isDefined)
      .toList

    // Configure Spark session
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("Team Schlau")
      .master(s"local[$cores]")
      .getOrCreate()

    sparkSession.conf.set("spark.sql.shuffle.partitions", "16")
    import sparkSession.implicits._

    // Read .csv-files
    val datasets: Seq[DataFrame] = files.map(file => {
      sparkSession.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(file.getAbsolutePath)
    })

    println("\nSTART\n")
    // Find inclusion dependencies
    val columnNameValueTuplesPerDataset:Seq[Dataset[(String, String)]] = datasets.map(dataset => {
      val columnNames = dataset.columns
      dataset.flatMap(row => {
        row.toSeq.zipWithIndex.map{case(cell, index) => (cell.toString, columnNames(index))}
      })
    })

    val columnNameValueTuples: Dataset[(String, String)] = columnNameValueTuplesPerDataset.reduce((datasetAccumulator, dataset) => datasetAccumulator.union(dataset))

    val attributeSets:RDD[(Set[String])] = columnNameValueTuples.rdd.combineByKey(createFromStringCombiner, mergeStringsToSet, mergeSets).map(entry => entry._2)

    val inclusionLists:RDD[(String, Set[String])] = attributeSets.flatMap(row => row.toSeq.map(entry => (entry, row.filter(a => !a.equals(entry)))))

    val aggregate:RDD[(String, Set[String])] = inclusionLists.combineByKey(createFromSetCombiner, mergeSets, mergeSets)


    aggregate.collect().map(entry => {
      println(entry._1 + " < " + entry._2.toString())
    })

    aggregate.collect().foreach(println)

  }
}