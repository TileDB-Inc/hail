package is.hail.expr.ir

import java.nio.file.{Path, Paths}

import is.hail.ExecStrategy.ExecStrategy
import is.hail.expr.SparkAnnotationImpex
import is.hail.types.physical.PStruct
import is.hail.types.virtual._
import is.hail.utils._
import is.hail.{ExecStrategy, HailSuite}
import org.apache.log4j.Level
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.testng.annotations.{DataProvider, Test}


class TileDBTest extends HailSuite {

  implicit val execStrats: Set[ExecStrategy] = Set(ExecStrategy.Interpret, ExecStrategy.InterpretUnoptimized, ExecStrategy.LoweredJVMCompile)

  @Test def testTileDBVCFDataFrame() {
    // TODO: Replace deprecated SQLContext class
    val session = new SQLContext(sc)

    val sampleGroupName = "ingested_2samples"
    val arraysPath = Paths.get("src", "test", "resources", "arrays", "v3", sampleGroupName)
    val path = "file://".concat(arraysPath.toAbsolutePath.toString)

    val tileDBVCFDataFrame = session.read.format("io.tiledb.vcf")
      .option("uri", path)
      .option("samples", "HG01762,HG00280")
      .option("ranges", "1:12100-13360,1:13500-17350")
      .option("tiledb.vfs.num_threads", 1)
      .option("tiledb_stats_log_level", Level.INFO.toString)
      .load

    // Get the TileDB-VCF schema
    val signature = SparkAnnotationImpex
      .importType(tileDBVCFDataFrame.schema)
      .setRequired(true)
      .asInstanceOf[PStruct]

    val keyNames = FastIndexedSeq.empty

    val tv = TableValue(ctx, signature.virtualType.asInstanceOf[TStruct], keyNames,
      tileDBVCFDataFrame.rdd, Some(signature))

    val tileDBRows = tileDBVCFDataFrame.collect().toFastIndexedSeq
    val hailRows = tv.toDF().collect().toFastIndexedSeq

    assert(tileDBRows == hailRows)
  }

  @Test def testTileDBVCFDataFrameProjection() {
    // TODO: Replace deprecated SQLContext class
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val sampleGroupName = "ingested_2samples"
    val arraysPath = Paths.get("src", "test", "resources", "arrays", "v3", sampleGroupName)
    val path = "file://".concat(arraysPath.toAbsolutePath.toString)

    val tileDBVCFDataFrame = sqlContext.read.format("io.tiledb.vcf")
      .option("uri", path)
      .option("samples", "HG01762,HG00280")
      .option("ranges", "1:12100-13360,1:13500-17350")
      .option("tiledb.vfs.num_threads", 1)
      .option("tiledb_stats_log_level", Level.INFO.toString)
      .load
      .select('fmt_SB, 'fmt_MIN_DP)

    // Get the TileDB-VCF schema
    val signature = SparkAnnotationImpex
      .importType(tileDBVCFDataFrame.schema)
      .setRequired(true)
      .asInstanceOf[PStruct]

    val keyNames = FastIndexedSeq("fmt_SB", "fmt_MIN_DP")

    val tv = TableValue(ctx, signature.virtualType.asInstanceOf[TStruct], keyNames,
      tileDBVCFDataFrame.rdd, Some(signature))

    val tileDBRows = tileDBVCFDataFrame
      .select('fmt_SB, 'fmt_MIN_DP)
      .orderBy('fmt_MIN_DP)
      .collect()
      .toFastIndexedSeq

    val hailRows = tv
      .toDF()
      .orderBy('fmt_MIN_DP)
      .collect()
      .toFastIndexedSeq

    assert(tileDBRows == hailRows)
  }
}
