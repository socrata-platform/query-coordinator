package com.socrata.querycoordinator.secondary_finder

import java.time.Instant

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.{AutomaticJsonCodec, WrapperJsonEncode, WrapperJsonDecode}
import com.rojoma.json.v3.util.time.ISO8601.codec._

case class PrimingData[DatasetName, Secondary](
  cache: Map[DatasetName, PrimingData.DatasetInfo[Secondary]]
)

object PrimingData {
  @AutomaticJsonCodec
  case class SecondaryInfo(
    present: CachedSecondaryInstanceFinder.CheckResult,
    checkedAt: Instant,
    checkAfter: Long
  )

  @AutomaticJsonCodec
  private case class WireDatasetInfo[Secondary](secondaries: Seq[(Secondary, SecondaryInfo)])

  case class DatasetInfo[Secondary](
    secondaries: Map[Secondary, SecondaryInfo]
  )
  object DatasetInfo {
    implicit def jEncode[Secondary: JsonEncode] =
      WrapperJsonEncode[DatasetInfo[Secondary]] { v =>
        WireDatasetInfo(v.secondaries.toSeq)
      }

    implicit def jDecode[Secondary: JsonDecode] =
      WrapperJsonDecode[DatasetInfo[Secondary]] { (v: WireDatasetInfo[Secondary]) =>
        DatasetInfo(v.secondaries.toMap)
      }
  }

  @AutomaticJsonCodec
  private case class WirePrimingData[DatasetName, Secondary](cache: Seq[(DatasetName, DatasetInfo[Secondary])])

  implicit def jEncode[DatasetName: JsonEncode, Secondary: JsonEncode] =
    WrapperJsonEncode[PrimingData[DatasetName, Secondary]] { v =>
      WirePrimingData(v.cache.toSeq)
    }

  implicit def jDecode[DatasetName: JsonDecode, Secondary: JsonDecode] =
    WrapperJsonDecode[PrimingData[DatasetName, Secondary]] { (v: WirePrimingData[DatasetName, Secondary]) =>
      PrimingData(v.cache.toMap)
    }
}
