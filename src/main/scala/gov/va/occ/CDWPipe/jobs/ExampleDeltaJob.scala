package gov.va.occ.CDWPipe.jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.log4j.LogManager

object ExampleDeltaJob {
  private val logger = LogManager.getLogger(getClass)

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val df: DataFrame = Seq(
      ("a", 1),
      ("b", 2)
    ).toDF("key", "value")
      .withColumn("ingest_ts", current_timestamp())

    // Minimal Arrow API usage (Spark bundles Arrow classes).
    // In Databricks, Spark supplies Arrow on the runtime classpath.
    arrowSanity()

    // Delta write example (path-based). In Databricks, point this to DBFS / Unity Catalog external location.
    val outPath = spark.conf.get("cdwpipe.delta.out", "dbfs:/tmp/cdwpipe/example_delta")

    df.write
      .format("delta")
      .mode("overwrite")
      .save(outPath)

    val readBack = spark.read.format("delta").load(outPath)
    val count = readBack.count()

    logger.info(s"Wrote and read back $count rows to $outPath")
  }

  private def arrowSanity(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val vec = new IntVector("value", allocator)
      try {
        vec.allocateNew(2)
        vec.setSafe(0, 1)
        vec.setSafe(1, 2)
        vec.setValueCount(2)

        logger.debug(s"Arrow IntVector valueCount=${vec.getValueCount}")
      } finally {
        vec.close()
      }
    } finally {
      allocator.close()
    }
  }
}
