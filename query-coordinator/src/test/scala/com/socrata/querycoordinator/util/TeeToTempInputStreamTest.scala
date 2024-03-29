package com.socrata.querycoordinator.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import com.rojoma.simplearm.v2._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FunSuite, MustMatchers}

class TeeToTempInputStreamTest extends FunSuite with MustMatchers with ScalaCheckPropertyChecks {
  private def readAll(in: InputStream): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val buf = new Array[Byte](1024)
    @annotation.tailrec
    def loop(): Unit = {
      in.read(buf) match {
        case -1 => // done
        case n: Int => baos.write(buf, 0, n); loop()
      }
    }
    loop()
    baos.toByteArray
  }

  private def read(in: InputStream, blockSize: Int, blockCount: Int): Int = {
    val trueBlockSize = blockSize & 0xff
    val trueBlockCount = blockCount & 0xf
    val bs = new Array[Byte](trueBlockSize)
    (1 to trueBlockCount).map(_ => in.read(bs)).filter(_ >= 0).sum
  }

  test("restream returns a stream equivalent to the original stream") {
    forAll { (xsR: Array[Array[Byte]], blockSize: Int, blockCount: Int) =>
      // Array[Array[Byte]] to make spill-to-disk more probable
      val xs = xsR.flatten
      val inputStream = new ByteArrayInputStream(xs)
      using(new TeeToTempInputStream(inputStream, inMemoryBufferSize = 128)) { ts =>
        val count = read(ts, blockSize, blockCount)
        using(ts.restream()) { rs =>
          readAll(rs) must equal(java.util.Arrays.copyOf(xs, count))
          readAll(inputStream) must equal(java.util.Arrays.copyOfRange(xs, count, xs.length))
        }
      }
    }
  }

  test("The restream remains valid even after the teeing stream is closed") {
    forAll { (xsR: Array[Array[Byte]], blockSize: Int, blockCount: Int) =>
      val xs = xsR.flatten
      val inputStream = new ByteArrayInputStream(xs)
      var count = 0
      using(using(new TeeToTempInputStream(inputStream, inMemoryBufferSize = 128)) { ts =>
        count = read(ts, blockSize, blockCount)
        ts.restream()
      }) { rs =>
        readAll(rs) must equal(java.util.Arrays.copyOf(xs, count))
        readAll(inputStream) must equal(java.util.Arrays.copyOfRange(xs, count, xs.length))
      }
    }
  }

  // TODO: test augment buffer by byte
  // TODO: test read after restream
  // TODO: test restream after restream
  // TODO: test close
}
