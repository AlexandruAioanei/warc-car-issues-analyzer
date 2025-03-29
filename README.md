# WARC Car Issues Analyzer

This repository contains a Scala application built with Apache Spark that analyzes WARC (Web ARChive) files to identify web pages mentioning car-related problems and their association with specific car brands.

## Project Overview

The application processes large-scale web data to:

1. Identify pages that mention common car problems (flat tires, transmission issues, engine problems, etc.)
2. Count and rank these pages based on the frequency of problem mentions
3. Associate car brands with these problems to determine which brands have the most issues reported online
4. Normalize results to account for differences in webpage length

## Requirements

- Apache Spark (2.12.x)
- Scala (2.12.x)
- SBT (Simple Build Tool)
- Jsoup (HTML Parser)
- Access to WARC files (Common Crawl)

## How to Build

Use the SBT assembly plugin to build a fat JAR with all dependencies:

```bash
sbt clean assembly
```

This will create a JAR file at `target/scala-2.12/CarIssuesAnalyzer-assembly-1.0.jar`

**Note:** Make sure to use `sbt assembly` rather than `sbt package` to properly include external dependencies like Jsoup.

## How to Run

The application can be run on a Spark cluster using the spark-submit command:

```bash
spark-submit --deploy-mode cluster --queue default target/scala-2.12/CarIssuesAnalyzer-assembly-1.0.jar
```

For better performance on larger datasets:

```bash
spark-submit --deploy-mode cluster --queue gold target/scala-2.12/CarIssuesAnalyzer-assembly-1.0.jar
```

## Configuration

You can modify the following in the code:

1. Number of WARC files to process:
   ```scala
   val warcFiles = (0 to 50).map(a => f"hdfs:/single-warc-segment/CC-MAIN-20210410105831-20210410135831-$a%05d.warc.gz")
   ```
   Adjust the range (e.g., `0 to 50`) based on your processing capacity and time constraints.

2. Car problem patterns:
   ```scala
   val carProblemsPattern = "(flat tire|transmission issues|squeaking brakes|electric battery problems|overheating|engine problems|radiator leaks|oil leaks|brake failure|alternator issues)".r
   ```

3. Car brand patterns:
   ```scala
   val carBrandsPattern = "(audi|bmw|mercedes[- ]benz|toyota|honda|ford|tesla|nissan|volkswagen)".r
   ```

4. Spark configuration for memory and performance optimization:
   ```scala
   val sparkConf = new SparkConf()
     .setAppName("CarIssuesAnalyzer")
     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .registerKryoClasses(Array(classOf[WarcRecord]))
     .set("spark.driver.memory", "4g")
     .set("spark.executor.memory", "8g")
     .set("spark.memory.offHeap.enabled", "true")
     .set("spark.memory.offHeap.size", "6g")
   ```

## Output

The application will output:

1. The top 10 web pages with the highest car problem mentions:
   - URL
   - Standardized word count (problem terms relative to page size)
   - Percentage representation
   - Raw problem count
   - Total word count

2. A list of car brands with their total problem mentions across all pages

Example output:
```
########## Start ##########
The best webpages in WARC files mentioning car-related problems (standardized by total word count):
URL: https://example.com/car-problems
Standardized Word Count: 0.01423
Standardized Word Count Percentage: 1.42%
Problem Count: 34
Total Word Count: 2388

Car brands with most problems based on total count:
ford: 1603
toyota: 535
bmw: 333
audi: 604
volkswagen: 98
mercedes-benz: 14
honda: 544
nissan: 66
########### Ending ############
```

## Performance Considerations

The application is optimized for processing large datasets:

- Uses Kryo serializer to reduce serialization overhead
- Configures memory settings for driver and executor
- Utilizes off-heap memory to reduce garbage collection overhead
- Filters out non-English pages to improve processing efficiency
- Implements efficient regex pattern matching

## Future Improvements

Potential enhancements:

- Implementation of context-aware analysis to reduce false positives
- Addition of more car brands and problem terms
- Integration with visualization tools
- Sentiment analysis to determine severity of reported problems
- Integration with car review datasets for cross-validation
