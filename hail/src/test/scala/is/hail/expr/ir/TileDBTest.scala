package is.hail.expr.ir

import io.tiledb.vcf.VCFDataSourceOptions

import java.net.URI
import java.nio.file.{Path, Paths}
import java.util
import is.hail.ExecStrategy.ExecStrategy
import is.hail.TestUtils.{assertEvalsTo, eval, importVCF}
import is.hail.annotations.Region
import is.hail.backend.spark.SparkBackend
import is.hail.expr.SparkAnnotationImpex
import is.hail.io.fs.HadoopFS
import is.hail.io.plink.{FamFileConfig, LoadPlink, MatrixPLINKReader, MatrixPLINKReaderParameters}
import is.hail.methods.LogisticRegression
import is.hail.rvd.RVDPartitioner
import is.hail.tiledb.TileDBHailVCFReader
import is.hail.types.{MatrixType, TableType}
import is.hail.types.physical.{PCanonicalCall, PInt32, PStruct}
import is.hail.types.virtual._
import is.hail.utils._
import is.hail.variant.{Locus, ReferenceGenome}
import is.hail.{ExecStrategy, HailContext, HailSuite}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Level
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.testng.annotations.{DataProvider, Test}

import scala.collection.mutable


class TileDBTest extends HailSuite {

  implicit val execStrats: Set[ExecStrategy] = Set(ExecStrategy.Interpret, ExecStrategy.InterpretUnoptimized, ExecStrategy.LoweredJVMCompile)

  val session = new SQLContext(sc)

  private def testSampleGroupURI(sampleGroupName: String) = {
    val arraysPath = Paths.get("/Users/victor/Dev/tiledb/TileDB-VCF/apis/spark/src/test/resources/arrays/v2", sampleGroupName)
    "file://".concat(arraysPath.toAbsolutePath.toString)
  }

  private def testSampleDataset = {
    val dfRead = session.read.format("io.tiledb.vcf").option("uri", testSampleGroupURI("ingested_2samples")).option("samples", "HG01762,HG00280").option("ranges", "1:12100-13360,1:13500-17350").option("tiledb.vfs.num_threads", 1).option("tiledb_stats_log_level", Level.INFO.toString).load
    dfRead
  }

  @Test
  def plinkReaderExample(): Unit = {
    val prefix = "/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/plink"
    val params = MatrixPLINKReaderParameters(s"${prefix}.bed", s"${prefix}.bim", s"${prefix}.fam", Some(1), Some(512), Some(1), "\t", null, false, false, Some("GRCh37"), Map[String, String](), false)

    val referenceGenome = params.rg.map(ReferenceGenome.getReference)
    referenceGenome.foreach(_.validateContigRemap(params.contigRecoding))

    val locusType = TLocus.schemaFromRG(referenceGenome)
    val locusAllelesType = TStruct(
      "locus" -> locusType,
      "alleles" -> TArray(TString))

    val ffConfig = FamFileConfig(params.quantPheno, params.delimiter, params.missing)

    val (sampleInfo, signature) = LoadPlink.parseFam(fs, params.fam, ffConfig)

    val nameMap = Map("id" -> "s")
    val saSignature = signature.copy(fields = signature.fields.map(f => f.copy(name = nameMap.getOrElse(f.name, f.name))))

    val fullMatrixType: MatrixType = MatrixType(
      globalType = TStruct.empty,
      colKey = Array("s"),
      colType = saSignature.virtualType,
      rowType = TStruct(
        "locus" -> locusType,
        "alleles" -> TArray(TString),
        "rsid" -> TString,
        "cm_position" -> TFloat64),
      rowKey = Array("locus", "alleles"),
      entryType = TStruct("GT" -> TCall))
    assert(locusAllelesType == fullMatrixType.rowKeyStruct)

    val (nTotalVariants, variants) = LoadPlink.parseBim(fs, params.bim, params.a2Reference, params.contigRecoding,
      referenceGenome, locusAllelesType, params.skipInvalidLoci)
    val nVariants = variants.length
    if (nTotalVariants <= 0)
      fatal("BIM file does not contain any variants")

    val nSamples = sampleInfo.length

    info(s"Found $nSamples samples in fam file.")
    info(s"Found $nTotalVariants variants in bim file.")

    using(fs.open(params.bed)) { dis =>
      val b1 = dis.read()
      val b2 = dis.read()
      val b3 = dis.read()

      if (b1 != 108 || b2 != 27)
        fatal("First two bytes of BED file do not match PLINK magic numbers 108 & 27")

      if (b3 == 0)
        fatal("BED file is in individual major mode. First use plink with --make-bed to convert file to snp major mode before using Hail")
    }

    val bedSize = fs.getFileSize(params.bed)
    if (bedSize != LoadPlink.expectedBedSize(nSamples, nTotalVariants))
      fatal("BED file size does not match expected number of bytes based on BIM and FAM files")

    var nPartitions = params.nPartitions match {
      case Some(nPartitions) => nPartitions
      case None =>
        val blockSizeInB = params.blockSizeInMB.getOrElse(128) * 1024 * 1024
        (nVariants + blockSizeInB - 1) / blockSizeInB
    }
    params.minPartitions match {
      case Some(minPartitions) =>
        if (nPartitions < minPartitions)
          nPartitions = minPartitions
      case None =>
    }
    // partitions non-empty
    if (nPartitions > nVariants)
      nPartitions = nVariants

    val partSize = partition(nVariants, nPartitions)
    val partScan = partSize.scanLeft(0)(_ + _)

    val cb = new ArrayBuilder[Any]()
    val ib = new ArrayBuilder[Interval]()

    var p = 0
    var prevEnd = 0
    val lOrd = locusType.ordering
    while (p < nPartitions) {
      val start = prevEnd

      var end = partScan(p + 1)
      if (start < end) {
        while (end + 1 < nVariants
          && lOrd.equiv(variants(end).locusAlleles.asInstanceOf[Row].get(0),
          variants(end + 1).locusAlleles.asInstanceOf[Row].get(0)))
          end += 1

        cb += Row(params.bed, start, end)

        ib += Interval(
          variants(start).locusAlleles,
          variants(end - 1).locusAlleles,
          includesStart = true, includesEnd = true)

        prevEnd = end
      }

      p += 1
    }
    assert(prevEnd == nVariants)

    val contexts = cb.result()

    val partitioner = new RVDPartitioner(locusAllelesType, ib.result(), 0)

    val plinkReader = new MatrixPLINKReader(params, referenceGenome, fullMatrixType, sampleInfo, variants, contexts, partitioner)

    val matrixRead = MatrixRead(plinkReader.fullMatrixType, false, false, plinkReader)
    val matrixRowsTable: TableIR = MatrixRowsTable(matrixRead)
    val plinkTableValue = Interpret.apply(matrixRowsTable, ctx)
  }

  @Test def hailReader(): Unit = {
    val optionsMap = new util.HashMap[String, String]
    optionsMap.put("uri", testSampleGroupURI("ingested_2samples"))
    optionsMap.put("samples", "HG01762,HG00280")
    optionsMap.put("ranges", "1:12100-13360,1:13500-17350")
    optionsMap.put("partitions", "10")
    optionsMap.put("tiledb.vfs.num_threads", "1")
    optionsMap.put("tiledb_stats_log_level", Level.INFO.toString)

    val dataSourceOptions = new DataSourceOptions(optionsMap)
    val opts = new VCFDataSourceOptions(dataSourceOptions)

    // Create the DF
    val df = testSampleDataset.toDF

    // Initialize the reader
    val reader = new TileDBHailVCFReader(null, null)

    val matrixRead = MatrixRead(reader.fullMatrixType, false, false, reader)
    val mt = MatrixRowsTable(matrixRead)

    // TableValue from our TileDB Helix Loader
    val tileDBTableValue = Interpret.apply(mt, ctx)

    val tableCount = TableCount.apply(mt)
    val countResult = hc.sparkBackend("yo").execute(new ExecutionTimer("executor"), tableCount, true)
    val mapRows = TableMapRows(mt, SelectFields(Ref("row", mt.typ.rowType), mt.typ.rowType.fieldNames.toSeq))
    val tableJoin = TableJoin(mapRows, mapRows, "inner", mt.typ.rowType.size)

    val joinResult = hc.sparkBackend("sb").execute(new ExecutionTimer("executor"), TableCount(tableJoin), true)

    System.out.println("Count: " + countResult)
    System.out.println("Join: " + joinResult)

    // Load the TileDB VCF file
    val vcf1 = importVCF(ctx, "/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/small.vcf")
    val t1: TableIR = MatrixRowsTable(vcf1)
    val tv1 = Interpret.apply(t1, ctx)

    // Load the Helix VCF file
    val vcf2 = importVCF(ctx, "src/test/resources/sample.vcf")
    val t2: TableIR = MatrixRowsTable(vcf2)
    val tv2 = Interpret.apply(t2, ctx)

  }

  @Test def hailReaderVCF(): Unit = {

    // Load the Helix VCF file
     val hailMatrixRead = importVCF(ctx, "/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/small.vcf")
     val hailMatrixRowsTable: TableIR = MatrixRowsTable(hailMatrixRead)
     val hailTV = Interpret.apply(hailMatrixRowsTable, ctx)

    hailTV.rvd.toRows.collect()

    val tiledbReader = TileDBHailVCFReader.apply(ctx, "/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/arrays/v3/ingested_2samples", Option("HG01762"))

    val tiledbMatrixRead = new MatrixRead(tiledbReader.fullMatrixType, false, false, tiledbReader)
    val tiledbMatrixRowsTable = MatrixRowsTable(tiledbMatrixRead)
    val tiledbTV = Interpret.apply(tiledbMatrixRowsTable, ctx)
  }

  @Test def testRDD(): Unit = {
    val optionsMap = new util.HashMap[String, String]
    optionsMap.put("uri", testSampleGroupURI("ingested_2samples"))
    optionsMap.put("samples", "HG01762,HG00280")
    optionsMap.put("ranges", "1:12100-13360,1:13500-17350")
    optionsMap.put("partitions", "10")
    optionsMap.put("tiledb.vfs.num_threads", "1")
    optionsMap.put("tiledb_stats_log_level", Level.INFO.toString)

    val dataSourceOptions = new DataSourceOptions(optionsMap)

    // Create the DF
    val df = testSampleDataset.toDF



    val transformedRDD = df.select("contig", "posStart", "alleles").rdd.map{
      r => Row(Locus(r.getString(0), r.getInt(1)), r.get(2).asInstanceOf[mutable.WrappedArray[Int]].toIndexedSeq)
    }

    val fullMatrixType: MatrixType = MatrixType(
      TStruct.empty,
      colType = TStruct("s" -> TString),
      colKey = Array("s"),
      rowType = TStruct("locus" -> TLocus(ReferenceGenome.GRCh37),
        "alleles" -> TArray(TString)),
      rowKey = Array("locus", "alleles"),
      entryType = TStruct.empty)

    val rowType = fullMatrixType.rowType
    val tt = TableType(rowType, rowType.fieldNames, TStruct.empty)
    val tp = TableValue(ctx, fullMatrixType.rowType, tt.key, transformedRDD)

    print()

    // Load the Helix VCF file
    val vcf2 = importVCF(ctx, "src/test/resources/sample.vcf")
    val t2: TableIR = MatrixRowsTable(vcf2)
    val tv2 = Interpret.apply(t2, ctx)

    tv2.toDF().show()

    print()

  }

  @Test def testTileDBVCFDataFrame() {
    // TODO: Replace deprecated SQLContext class

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

  @Test def testLR() {
    val df = ctx.backend.asSpark("TileDBHailVCFReader").sparkSession.read
      .format("io.tiledb.vcf")
      .option("uri", "/Users/victor/Dev/tiledb/TileDB-VCF/apis/spark/src/test/resources/arrays/v3/ingested_2samples")
      .option("samples", "HG01762")
      .option("tiledb.vfs.num_threads", 1)
      .option("tiledb_stats_log_level", Level.INFO.toString)
      .load()

    df.show()
    println()
  }
}
