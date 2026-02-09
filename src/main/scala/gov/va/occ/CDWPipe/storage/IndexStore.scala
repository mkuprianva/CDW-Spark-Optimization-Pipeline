package gov.va.occ.CDWPipe.storage

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import gov.va.occ.CDWPipe.structs.index.{IndexTracker, indexWrapper}
import gov.va.occ.CDWPipe.utils.IndexSerializer

/**
  * Persistence layer for [[IndexTracker]] and [[indexWrapper]] objects.
  *
  * Uses the Hadoop [[FileSystem]] API so it works transparently with any
  * scheme the cluster supports:
  *
  *  - '''ADLS Gen2''': `abfss://container@account.dfs.core.windows.net/path`
  *  - '''DBFS''':      `dbfs:/path`
  *  - '''S3''':        `s3a://bucket/path`
  *  - '''Local''':     `file:///tmp/path`
  *
  * On Databricks the storage credentials are resolved from the cluster or
  * Unity Catalog configuration—no extra setup is needed here.
  *
  * Convention: files are saved with the `.cdwidx` extension.
  *
  * ==Example==
  * {{{
  * val basePath = "abfss://data@myaccount.dfs.core.windows.net/indices"
  *
  * // Save
  * IndexStore.save(spark, tracker, s"$basePath/patient-join.cdwidx")
  *
  * // Load
  * val loaded = IndexStore.load(spark, s"$basePath/patient-join.cdwidx")
  *
  * // List all saved trackers
  * val paths = IndexStore.list(spark, basePath)
  *
  * // Delete
  * IndexStore.delete(spark, s"$basePath/patient-join.cdwidx")
  * }}}
  */
object IndexStore {

  val Extension = ".cdwidx"

  // ───────── IndexTracker persistence ─────────

  /**
    * Save an [[IndexTracker]] to the given path.
    *
    * If the file already exists it will be '''overwritten'''.
    *
    * @param spark   active SparkSession (provides Hadoop configuration)
    * @param tracker the tracker to persist
    * @param path    full path including filename, e.g.
    *                `abfss://container@acct.dfs.core.windows.net/idx/my.cdwidx`
    */
  def save(spark: SparkSession, tracker: IndexTracker, path: String): Unit = {
    val bytes  = IndexSerializer.serialize(tracker)
    val fsPath = new Path(path)
    val fs     = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val out    = fs.create(fsPath, true) // overwrite = true
    try {
      out.write(bytes)
    } finally {
      out.close()
    }
  }

  /**
    * Load an [[IndexTracker]] from the given path.
    *
    * @throws java.io.FileNotFoundException if the path does not exist
    */
  def load(spark: SparkSession, path: String): IndexTracker = {
    val fsPath = new Path(path)
    val fs     = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val status = fs.getFileStatus(fsPath)
    val len    = status.getLen.toInt
    val buf    = new Array[Byte](len)

    val in = fs.open(fsPath)
    try {
      in.readFully(buf)
    } finally {
      in.close()
    }

    IndexSerializer.deserialize(buf)
  }

  /**
    * Check whether a tracker file exists at the given path.
    */
  def exists(spark: SparkSession, path: String): Boolean = {
    val fsPath = new Path(path)
    val fs     = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(fsPath)
  }

  /**
    * Delete a tracker file.  Returns `true` if the file was deleted,
    * `false` if it did not exist.
    */
  def delete(spark: SparkSession, path: String): Boolean = {
    val fsPath = new Path(path)
    val fs     = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.delete(fsPath, false) // recursive = false (single file)
  }

  /**
    * List all `.cdwidx` files directly under `dirPath`.
    *
    * @return full paths of matching files, sorted lexicographically
    */
  def list(spark: SparkSession, dirPath: String): Seq[String] = {
    val fsPath = new Path(dirPath)
    val fs     = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(fsPath)) return Seq.empty

    fs.listStatus(fsPath)
      .filter(s => !s.isDirectory && s.getPath.getName.endsWith(Extension))
      .map(_.getPath.toString)
      .sorted
      .toSeq
  }

  // ───────── Standalone indexWrapper persistence ─────────

  /** Save just an [[indexWrapper]] (without the full tracker metadata). */
  def saveWrapper(spark: SparkSession, wrapper: indexWrapper, path: String): Unit = {
    val bytes  = IndexSerializer.serializeWrapper(wrapper)
    val fsPath = new Path(path)
    val fs     = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val out    = fs.create(fsPath, true)
    try {
      out.write(bytes)
    } finally {
      out.close()
    }
  }

  /** Load just an [[indexWrapper]]. */
  def loadWrapper(spark: SparkSession, path: String): indexWrapper = {
    val fsPath = new Path(path)
    val fs     = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val status = fs.getFileStatus(fsPath)
    val len    = status.getLen.toInt
    val buf    = new Array[Byte](len)

    val in = fs.open(fsPath)
    try {
      in.readFully(buf)
    } finally {
      in.close()
    }

    IndexSerializer.deserializeWrapper(buf)
  }
}
