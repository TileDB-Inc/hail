package is.hail.tiledb

import io.tiledb.libvcfnative.VCFReader
import io.tiledb.libvcfnative.VCFReader.AttributeDatatype
import is.hail.annotations.BroadcastRow
import is.hail.expr.ir.{ExecuteContext, LowerMatrixIR, MatrixHybridReader, TableRead, TableValue}
import is.hail.rvd.{RVD, RVDType}
import is.hail.sparkextras.ContextRDD
import is.hail.types.physical.{PCanonicalCall, PCanonicalString, PField, PFloat32, PInt32, PStruct, PType}
import is.hail.types.virtual._
import is.hail.types.{MatrixType, TableType}
import is.hail.utils.{FastIndexedSeq, toRichContextRDDRow, toRichIterable}
import is.hail.variant.{Call, Call2, Locus, ReferenceGenome}
import org.apache.spark.sql.{Column, Row, functions}
import org.json4s.JsonAST.JValue
import org.json4s.{DefaultFormats, Formats}

import java.util.Optional
import scala.collection.mutable


class TileDBHailVCFReader(val uri: String, val samples: Option[String]) extends MatrixHybridReader {
  override def pathsUsed: Seq[String] = Seq.empty
  override def columnCount: Option[Int] = None
  override def partitionCounts: Option[IndexedSeq[Long]] = None

  val sampleList = {
    if (samples.isDefined) samples.get.split(",")
    else Array[String]()
  }

  val vcfReader: VCFReader = new VCFReader(uri, sampleList, Optional.empty(), Optional.empty())

  val fmt = vcfReader.fmtAttributes.keySet.toArray.map{ fmtAttr =>
    val key = fmtAttr.toString
    val dt = {
      if (key.contains("GT")) {
        PCanonicalCall()
      }
      else {
        vcfReader.fmtAttributes.get(key).datatype match {
          case AttributeDatatype.UINT8 => PInt32()
          case AttributeDatatype.INT32 => PInt32()
          case AttributeDatatype.CHAR => PCanonicalString()
          case AttributeDatatype.FLOAT32 => PFloat32()
        }
      }
    }
    (fmtAttr.toString.split("_")(1) -> dt.virtualType)
  }

  val info = vcfReader.infoAttributes.keySet.toArray.map{ infoAttr =>
    val key = infoAttr.toString
    val dt = {
      if (key.contains("GT"))
        PCanonicalCall(false)
      else {
        vcfReader.infoAttributes.get(key).datatype match {
          case AttributeDatatype.UINT8 => PInt32()
          case AttributeDatatype.INT32 => PInt32()
          case AttributeDatatype.CHAR => PCanonicalString()
          case AttributeDatatype.FLOAT32 => PFloat32()
        }
      }
    }
    (infoAttr.toString.split("_")(1) -> dt.virtualType)
  }

  val entryType = TStruct("DP" -> PInt32().virtualType, "GT" -> PCanonicalCall().virtualType, "PL" -> PInt32().virtualType)

  val fullMatrixType: MatrixType = MatrixType(
    globalType = TStruct.empty,
    colType = TStruct("s" -> TString),
    colKey = Array("s"),
    rowType = TStruct(
      "locus" -> TLocus(ReferenceGenome.GRCh37),
      "alleles" -> TArray(TString),
      "info" -> entryType),
    rowKey = Array("locus", "alleles"),
    entryType = entryType)

  val str = ""

  override def apply(tr: TableRead, ctx: ExecuteContext): TableValue = {
    val requestedType = tr.typ

    val df = ctx.backend.asSpark("spark")
      .sparkSession.read
      .format("io.tiledb.vcf")
      .option("samples", samples.get)
      .option("uri", uri)
      .load()

    val df2 = df.groupBy("contig","posStart", "alleles")
      .agg(functions.collect_list("genotype").as("GT"),
        functions.collect_list("fmt_DP").as("DP"))


    val tt = fullMatrixType.toTableType(LowerMatrixIR.entriesFieldName, LowerMatrixIR.colsFieldName)

    val rdd = df2.rdd.map{
      r => {
        val locus = Locus(r.getString(0), r.getInt(1))
        val alleles = r.get(2)

        val GT = r.get(3).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[Int]]].map(x => Row(Call2(x(0), x(1))))

        Row(locus, alleles, GT)
      }
    }

    val globals = {
      if (samples.isDefined)
        Row(samples.get.split(",").map(Row(_)).toFastIndexedSeq)
      else
        Row(FastIndexedSeq.empty)
    }
    val g = { (requestedGlobalsType: Type) =>
      val subset = tt.globalType.valueSubsetter(requestedGlobalsType)
      subset(globals).asInstanceOf[Row]
    }

    val crdd = ContextRDD.weaken(rdd).toRegionValues(requestedType.canonicalRowPType)

    val rvd = RVD.coerce(ctx, requestedType.canonicalRVDType, crdd)

    val tv = TableValue(ctx,
      requestedType,
      BroadcastRow(ctx, g(requestedType.globalType), requestedType.globalType),
      rvd)

    //val tv = TableValue(ctx, tr.typ.rowType, tr.typ.key, rdd)

    println(s"Created TileDB table with ${tr}")
    tv.rdd.collect.foreach(println)
    tv
  }

  override def rowAndGlobalPTypes(ctx: ExecuteContext, requestedType: TableType): (PStruct, PStruct) = {
    requestedType.canonicalRowPType -> PType.canonical(requestedType.globalType).asInstanceOf[PStruct]
  }
}

case class TileDBHail(uri: String, samples: String)

object TileDBHailVCFReader {
  def fromJValue(jv: JValue) = {
    implicit val formats: Formats = DefaultFormats
    val params = jv.extract[TileDBHail]

    new TileDBHailVCFReader(params.uri, Option(params.samples))
  }

  def apply(ctx: ExecuteContext, uri: String, samples: Option[String]): TileDBHailVCFReader = {
    new TileDBHailVCFReader(uri, samples)
  }

  def apply(ctx: ExecuteContext, uri: String): TileDBHailVCFReader = {
    apply(ctx, uri, Option.empty)
  }
}