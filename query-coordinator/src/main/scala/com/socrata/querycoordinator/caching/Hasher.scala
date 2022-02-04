package com.socrata.querycoordinator.caching

import scala.language.implicitConversions
import scala.collection.immutable.{SortedMap, SortedSet}
import java.nio.charset.StandardCharsets
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.security.MessageDigest
import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.soql.stdlib.{Context, UserContext}
import com.socrata.soql.types.SoQLFloatingTimestamp

object Hasher {
  trait ImplicitlyByteable {
    def asBytes: Array[Byte]
  }

  object ImplicitlyByteable {
    implicit def implicitlyByteable(s: String): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = s.getBytes(StandardCharsets.UTF_8)
    }

    implicit def implicitlyByteable(bs: Array[Byte]): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = bs.clone()
    }

    implicit def implicitlyBytable(optStr: Option[String]): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = optStr.toString.getBytes(StandardCharsets.UTF_8)
    }

    implicit def implicitlyBytable(n: Long): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = {
        val os = new Array[Byte](8)
        os(0) = (n >> 56).toByte
        os(1) = (n >> 48).toByte
        os(2) = (n >> 40).toByte
        os(3) = (n >> 32).toByte
        os(4) = (n >> 24).toByte
        os(5) = (n >> 16).toByte
        os(6) = (n >> 8).toByte
        os(7) = n.toByte
        os
      }
    }

    implicit def implicitlyByteable(ctx: Context): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = {
        val baos = new ByteArrayOutputStream
        val dos = new DataOutputStream(baos)

        val Context(system, UserContext(text, bool, num, float, fixed)) = ctx

        // n.b., 255 and 254 are bytes that do not appear in UTF-8-encoded text

        dos.writeInt(system.size)
        for(k <- system.keys.to[SortedSet]) {
          dos.write(k.getBytes(StandardCharsets.UTF_8))
          dos.write(255)
          dos.write(system(k).getBytes(StandardCharsets.UTF_8))
          dos.write(255)
        }

        dos.writeInt(text.size)
        for(k <- text.keys.to[SortedSet]) {
          dos.write(k.getBytes(StandardCharsets.UTF_8))
          dos.write(255)
          dos.write(text(k).value.getBytes(StandardCharsets.UTF_8))
          dos.write(255)
        }

        dos.writeInt(bool.size)
        for(k <- bool.keys.to[SortedSet]) {
          dos.write(k.getBytes(StandardCharsets.UTF_8))
          dos.write(if(bool(k).value) 255 else 254)
        }

        dos.writeInt(num.size)
        for(k <- num.keys.to[SortedSet]) {
          dos.write(k.getBytes(StandardCharsets.UTF_8))
          dos.write(255)
          dos.write(num(k).toString.getBytes(StandardCharsets.UTF_8)) // ick
          dos.write(255)
        }

        dos.writeInt(float.size)
        for(k <- float.keys.to[SortedSet]) {
          dos.write(k.getBytes(StandardCharsets.UTF_8))
          dos.write(255)
          dos.write(SoQLFloatingTimestamp.StringRep(float(k).value).getBytes(StandardCharsets.UTF_8)) // blech
          dos.write(255)
        }

        dos.writeInt(fixed.size)
        for(k <- fixed.keys.to[SortedSet]) {
          dos.write(k.getBytes(StandardCharsets.UTF_8))
          dos.write(255)
          dos.writeLong(fixed(k).value.getMillis) // fixed size, doesn't need a terminator
        }

        dos.flush()
        baos.toByteArray
      }
    }
  }

  def hash(items: ImplicitlyByteable*): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    val lenBuf = new Array[Byte](4)
    items.foreach { item =>
      val bs = item.asBytes
      val len = bs.length
      lenBuf(0) = (len >> 24).toByte
      lenBuf(1) = (len >> 16).toByte
      lenBuf(2) = (len >> 8).toByte
      lenBuf(3) = len.toByte
      md.update(lenBuf)
      md.update(bs)
    }
    md.digest()
  }
}
