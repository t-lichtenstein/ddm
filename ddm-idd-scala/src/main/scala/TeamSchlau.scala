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
    /* Parse Input Argument
    Default: path=./TPCH | cores=4
    */
    var cores = 4
    var datasetPath = "./TPCH"
    args.sliding(2, 2).toList.collect {
      case Array("--cores", argCores: String) => cores = argCores.toInt
      case Array("--path", argPath: String) => datasetPath = argPath
    }

    // Gather all CSV-Files in the Specified Path as List
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

    //session.conf.set("spark.sql.shuffle.partitions", "16")

    import session.implicits._
    val startTime = System.currentTimeMillis();

    // Read CSV-Files into a DataFrame-Format
    val datasets: Seq[DataFrame] = files.map(file => {
      session.read
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(file.getAbsolutePath)
    })

    /* Find Inclusion Dependencies
    The general procedure is taken from the paper "Scaling Out the Discovery of Inclusion Dependencies" introduced in the lecture.
    * */

    // Create the (Cell, ColumnName)-Pairs from the Input Tuples
    val datasetColumnNameValueTuples:Seq[Dataset[(String, String)]] = datasets.map(dataset => {
      val columnNames = dataset.columns
      dataset.flatMap(row => {
        row.toSeq.zipWithIndex.map{case(cell, index) => (cell.toString, columnNames(index))}
      })
    })

    val columnNameValueTuples: Dataset[(String, String)] = datasetColumnNameValueTuples
      .reduce((datasetAccumulator, dataset) => datasetAccumulator.union(dataset))

    // Convert the (Cell, ColumnName)-Pairs to the Attribute Sets by Cache-Based Preaggregation and Global Partitioning
    val attributeSets:RDD[(Set[String])] = columnNameValueTuples
      .rdd
      .combineByKey(createSetFromStringCombiner, mergeStringsToSet, mergeSets)
      .map(entry => entry._2)

    // Create the Inclusion Lists from the Attribute Sets
    val inclusionLists:RDD[(String, Set[String])] = attributeSets
      .flatMap(row => row.toSeq.map(entry => (entry, row.filter(a => !a.equals(entry)))))

    // Partition and Aggregate the Inclusion Lists as IND's
    val aggregatedDependencies:RDD[(String, Set[String])] = inclusionLists
      .combineByKey(createSetFromSetCombiner, intersectSets, intersectSets)

    // Filter IND's for Empty Entries and Sort them Lexicographically
    val sortedDependencies:RDD[(String, Set[String])] = aggregatedDependencies
      .filter(row => !row._2.isEmpty)
      .sortBy(_._1)

    // Format and Print the resulting IND's
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