import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.collection.immutable.SortedSet

object TeamSchlau {
  // Combination helper methods
  def createSetFromStringCombiner = (value: (String)) => Set(value)

  def createSetFromSetCombiner = (value: (Set[String])) => value

  def mergeStringsToSet = (accumulator: Set[String], element: String) =>
    (accumulator + element)

  def mergeSets = (accumulator1: (Set[String]), accumulator2: (Set[String])) =>
    (accumulator1.union(accumulator2))

  def intersectSets = (accumulator1: (Set[String]), accumulator2: (Set[String])) =>
    (accumulator1.intersect(accumulator2))

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
    val datasetPath = "./data/TPCH"

    // Get all .csv-file paths in the dataset path
    val fileRegex = """.*\.csv$""".r
    val directory = new File(datasetPath)
    val files = directory
      .listFiles
      .filter(_.isFile)
      .filter(file => fileRegex.findFirstIn(file.getName()).isDefined)
      .toList

    // Configure Spark session
    val session: SparkSession = SparkSession
      .builder()
      .appName("Team Schlau")
      .master(s"local[$cores]")
      .getOrCreate()

    session.conf.set("spark.sql.shuffle.partitions", "16")
    import session.implicits._

    val startTime = System.currentTimeMillis();

    // Read .csv-files
    val datasets: Seq[DataFrame] = files.map(file => {
      session.read
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(file.getAbsolutePath)
    })

    // Find inclusion dependencies

    // Get all value, column name tuples for each dataset
    val datasetColumnNameValueTuples:Seq[Dataset[(String, String)]] = datasets.map(dataset => {
      val columnNames = dataset.columns
      dataset.flatMap(row => {
        row.toSeq.zipWithIndex.map{case(cell, index) => (cell.toString, columnNames(index))}
      })
    })

    // Merge all sequences of tuples
    val columnNameValueTuples: Dataset[(String, String)] = datasetColumnNameValueTuples
      .reduce((datasetAccumulator, dataset) => datasetAccumulator.union(dataset))

    // Get all attribute sets
    val attributeSets:RDD[(Set[String])] = columnNameValueTuples
      .rdd
      .combineByKey(createSetFromStringCombiner, mergeStringsToSet, mergeSets)
      .map(entry => entry._2)

    // Generate subset entries for each entry
    val inclusionLists:RDD[(String, Set[String])] = attributeSets
      .flatMap(row => row.toSeq.map(entry => (entry, row.filter(a => !a.equals(entry)))))

    // Merge all entries by intersection
    val aggregatedDependencies:RDD[(String, Set[String])] = inclusionLists
      .combineByKey(createSetFromSetCombiner, intersectSets, intersectSets)

    // Filter entries with no dependency and sort all entries
    val sortedDependencies:RDD[(String, Set[String])] = aggregatedDependencies
      .filter(row => !row._2.isEmpty)
      .sortBy(_._1)

    // Format and print the result
    sortedDependencies
      .collect()
      .map(entry => {
        val sortedDependencies = SortedSet[String]() ++ entry._2
        val dependencyString = sortedDependencies.reduce((acc, entry) => {
          if (acc.equals("")) {
            acc + entry
          } else {
            acc + ", " + entry
          }
        })
        println(entry._1 + " < " + dependencyString)
      })

    println("DONE IN " +
      (System.currentTimeMillis() - startTime) / 1000.0 + " s (" +
      (System.currentTimeMillis() - startTime) / 60000.0 + " min)")
  }
}