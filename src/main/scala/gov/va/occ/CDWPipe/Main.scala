package gov.va.occ.CDWPipe

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CDWPipe")
      // For local runs only; Databricks overrides master.
      .getOrCreate()

    try {
      jobs.ExampleDeltaJob.run(spark)
    } finally {
      spark.stop()
    }
  }
}
