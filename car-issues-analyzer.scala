package org.caranalytics

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat, WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.jsoup.Jsoup
import scala.collection.JavaConverters._
import org.apache.spark.sql._

/**
 * An application for analyzing car-related issues mentioned in web content
 * using Apache Spark to process WARC (Web Archive) files.
 */
object CarIssuesAnalyzer {
  def main(args: Array[String]) {

    // Spark configuration with performance optimizations
    val sparkConf = new SparkConf()
      .setAppName("CarIssuesAnalyzer")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[WarcRecord]))
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "8g")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "6g")

    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    // Define the WARC files to read
    // Default: process 50 WARC files, can be adjusted based on available computing resources
    val warcFiles = (0 to 50).map(a => 
      f"hdfs:/single-warc-segment/CC-MAIN-20210410105831-20210410135831-$a%05d.warc.gz"
    )
    
    // Regular expression patterns for car-related problems and brands
    val carProblemsPattern = "(flat tire|transmission issues|squeaking brakes|electric battery problems|overheating|engine problems|radiator leaks|oil leaks|brake failure|alternator issues)".r
    val carBrandsPattern = "(audi|bmw|mercedes[- ]benz|toyota|honda|ford|tesla|nissan|volkswagen)".r

    /**
     * Processes a single WARC record to extract car-related information
     * 
     * @param wr WARC record to process
     * @return Tuple containing URL, standardized count, problem count, word count, and brand occurrences
     */
    def processWarcRecord(wr: WarcRecord): (String, Double, Int, Int, Map[String, Int]) = {
      val bodyText = Jsoup.parse(wr.getHttpStringBody()).select("body").text().toLowerCase
      val totalWordCount = bodyText.split("\\s+").length
      val problemCount = carProblemsPattern.findAllIn(bodyText).toList.length
      val brandCount = carBrandsPattern.findAllIn(bodyText).toList.groupBy(identity).mapValues(_.size)
      val standardizedCount = if (totalWordCount > 0) problemCount.toDouble / totalWordCount else 0.0
      (wr.getHeader().getUrl(), standardizedCount, problemCount, totalWordCount, brandCount)
    }

    // Read and process WARC files
    val warcRecords = sc.newAPIHadoopFile(
      warcFiles.mkString(","),
      classOf[WarcGzInputFormat],
      classOf[NullWritable],
      classOf[WarcWritable]
    ).map { wr => wr._2.getRecord() }
      .filter(wr => wr.getHeader().getUrl() != null && wr.getHttpStringBody() != null)
      .filter(wr => {
        val doc = Jsoup.parse(wr.getHttpStringBody())
        val lang = doc.select("html").attr("lang")
        lang == null || lang.isEmpty || lang.startsWith("en") // Filter for English content only
      })

    // Process only records with at least one car problem mentioned
    val processedRecords = warcRecords.map(processWarcRecord).filter(_._3 > 0)

    // Create DataFrame from the results for SQL querying
    val df = spark.createDataFrame(processedRecords).toDF("url", "standardized_wordcount", "problem_count", "total_wordcount", "brand_count")
    df.createOrReplaceTempView("df")

    // Query to get the top 10 web pages by problem count
    val topPages = spark.sql(
      "SELECT url, standardized_wordcount, problem_count, total_wordcount, brand_count FROM df ORDER BY problem_count DESC LIMIT 10"
    ).collect()

    // Print the results
    println("\n########## Analysis Results ##########")
    println("Top webpages mentioning car-related problems (standardized by total word count):")
    topPages.foreach { row =>
      val standardizedWordCount = row.getDouble(1)
      val standardizedPercentage = standardizedWordCount * 100 // Convert to percentage
      println(s"URL: ${row.getString(0)}")
      println(f"Standardized Word Count: $standardizedWordCount%.5f")
      println(f"Standardized Word Count Percentage: $standardizedPercentage%.2f%%")
      println(s"Problem Count: ${row.getInt(2)}")
      println(s"Total Word Count: ${row.getInt(3)}")
      println()
    }

    // Aggregate brand mentions across all pages
    val allBrandMentions = processedRecords.flatMap { case (_, _, _, _, brandCount) =>
      brandCount.toList
    }.groupBy(_._1)
      .mapValues(_.map(_._2).sum)
      .collect()
      .toList

    // Print the total counts for each brand
    println("Car brands with most problems based on total count:")
    allBrandMentions.foreach { case (brand, totalCount) =>
      println(s"$brand: $totalCount")
    }
    println("########### End of Analysis ############\n")

    spark.stop()
  }
}
