package com.socrata.querycoordinator.util

import scala.util.parsing.input.NoPosition

import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode, DecodeError}
import com.rojoma.json.v3.util.AutomaticJsonCodec
import com.socrata.soql.environment.{Source, ScopedResourceName}
import com.socrata.soql.util.{EncodedError, SoQLErrorCodec, SoQLErrorDecode, SoQLErrorEncode}

import com.socrata.querycoordinator.resources.NewQueryResource.{DatasetInternalName, Stage}

sealed abstract class NewQueryError[+RNS]

object NewQueryError {
  sealed abstract class SecondarySelectionError[+RNS] extends NewQueryError[RNS]

  object SecondarySelectionError {
    private def stringifyDS[RNS](name: ScopedResourceName[RNS]): String =
      s"${name.name.name}"

    case class NoSecondariesForDataset[+RNS](name: ScopedResourceName[RNS])
        extends SecondarySelectionError[RNS]

    object NoSecondariesForDataset {
      private val tag = "soql.query-coordinator.no-secondaries-for-dataset"

      @AutomaticJsonCodec
      case class Fields()

      implicit def encode[RNS: JsonEncode] =
        new SoQLErrorEncode[NoSecondariesForDataset[RNS]] {
          override val code = tag

          def encode(err: NoSecondariesForDataset[RNS]) =
            result(
              Fields(),
              s"No secondaries available for dataset ${stringifyDS(err.name)}",
              Source.Saved(err.name, NoPosition)
            )
        }

      implicit def decode[RNS: JsonDecode] =
        new SoQLErrorDecode[NoSecondariesForDataset[RNS]] {
          override val code = tag

          def decode(v: EncodedError) =
            for {
              _ <- data[Fields](v)
              source <- source[RNS](v)
              srn <- source.scopedResourceName match {
                case Some(srn) => Right(srn)
                case None => Left(DecodeError.InvalidValue(JsonEncode.toJValue(v.source.get))) // we know there's _a_ source because we just got it
              }
            } yield {
              NoSecondariesForDataset(srn)
            }
        }
    }

    case class NoSecondariesForAllDatasets()
        extends SecondarySelectionError[Nothing]


    object NoSecondariesForAllDatasets {
      private val tag = "soql.query-coordinator.no-secondaries-for-all-datasets"

      @AutomaticJsonCodec
      case class Fields()

      implicit def encode[RNS: JsonEncode] =
        new SoQLErrorEncode[NoSecondariesForAllDatasets] {
          override val code = tag

          def encode(err: NoSecondariesForAllDatasets) =
            result(
              Fields(),
              s"No secondaries in common for all datasets.  Perhaps they need to be collocated."
            )
        }

      implicit def decode =
        new SoQLErrorDecode[NoSecondariesForAllDatasets] {
          override val code = tag

          def decode(v: EncodedError) =
            for {
              _ <- data[Fields](v)
            } yield {
              NoSecondariesForAllDatasets()
            }
        }
    }

    def errorCodecs[RNS: JsonEncode: JsonDecode, T >: SecondarySelectionError[RNS] <: AnyRef](
      codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
    ) = {
      codecs
        .branch[NoSecondariesForDataset[RNS]]
        .branch[NoSecondariesForAllDatasets]
    }
  }

  def errorCodecs[RNS: JsonEncode: JsonDecode, T >: NewQueryError[RNS] <: AnyRef](
    codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
  ) = {
    SecondarySelectionError.errorCodecs[RNS, T](codecs)
  }
}
