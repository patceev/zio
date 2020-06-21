package zio.stream.compression

import java.util.zip.{ CRC32, Inflater }
import java.{ util => ju }

import scala.annotation.tailrec

import zio._
import java.util.zip.DataFormatException

private[compression] class Gunzipper private (bufferSize: Int) {

  import Gunzipper._

  private val inflater     = new Inflater(true)
  private var state: State = new ParseHeaderStep(Array.empty)

  def reset(): Unit = inflater.reset()

  def close(): Unit = inflater.end()

  def onChunk(c: Chunk[Byte]): ZIO[Any, CompressionException, Chunk[Byte]] =
    ZIO.effect {
      val (newState, output) = state.feed(c)
      state = newState
      output
    }.refineOrDie {
      case e: DataFormatException  => CompressionException(e)
      case e: CompressionException => e
    }

  private def nextStep(
    parsedBytes: Array[Byte],
    checkCrc16: Boolean,
    parseExtra: Boolean,
    commentsToSkip: Int
  ): Gunzipper.State =
    if (parseExtra) new ParseExtraStep(parsedBytes, checkCrc16, commentsToSkip)
    else if (commentsToSkip > 0) new SkipCommentsStep(parsedBytes, checkCrc16, commentsToSkip)
    else if (checkCrc16) new CheckCrc16Step(parsedBytes, Array.empty)
    else new Decompress()

  private class ParseHeaderStep(oldBytes: Array[Byte]) extends State {

    //TODO: If whole input is shorther than fixed header, not output is produced and no error is singaled. Is it ok?
    def feed: Chunk[Byte] => (State, Chunk[Byte]) = { c =>
      val bytes = oldBytes ++ c.toArray[Byte]
      if (bytes.length < fixedHeaderLength) {
        if (bytes.length == oldBytes.length && c.length > 0)
          throw CompressionException("Invalid GZIP header")
        else (new ParseHeaderStep(bytes), Chunk.empty)
      } else {
        val (header, rest) = bytes.splitAt(fixedHeaderLength)
        if (u8(header(0)) != 31 || u8(header(1)) != 139) throw CompressionException("Invalid GZIP header")
        else if (header(2) != 8)
          throw CompressionException(s"Only deflate (8) compression method is supported, present: ${header(2)}")
        else {
          val flags           = header(3) & 0xff
          val checkCrc16      = (flags & 2) > 0
          val hasExtra        = (flags & 4) > 0
          val skipFileName    = (flags & 8) > 0
          val skipFileComment = (flags & 16) > 0
          val commentsToSkip  = (if (skipFileName) 1 else 0) + (if (skipFileComment) 1 else 0)
          nextStep(header, checkCrc16, hasExtra, commentsToSkip).feed(Chunk.fromArray(rest))
        }
      }
    }
  }

  private class ParseExtraStep(bytes: Array[Byte], checkCrc16: Boolean, commentsToSkip: Int) extends State {

    def feed: Chunk[Byte] => (Gunzipper.State, Chunk[Byte]) =
      c => {
        val header = bytes ++ c.toArray[Byte]
        if (header.length < 12) {
          (new ParseExtraStep(header, checkCrc16, commentsToSkip), Chunk.empty)
        } else {
          val extraBytes: Int       = u16(header(fixedHeaderLength), header(fixedHeaderLength + 1))
          val headerWithExtraLength = fixedHeaderLength + extraBytes
          if (header.length < headerWithExtraLength)
            (new ParseExtraStep(header, checkCrc16, commentsToSkip), Chunk.empty)
          else {
            val (headerWithExtra, rest) = header.splitAt(headerWithExtraLength)
            nextStep(headerWithExtra, checkCrc16, false, commentsToSkip).feed(Chunk.fromArray(rest))
          }
        }
      }
  }

  private class SkipCommentsStep(pastBytes: Array[Byte], checkCrc16: Boolean, commentsToSkip: Int) extends State {
    def feed: Chunk[Byte] => (State, Chunk[Byte]) =
      c => {
        val idx           = c.indexWhere(_ == 0)
        val (upTo0, rest) = if (idx == -1) (c, Chunk.empty) else c.splitAt(idx + 1)
        nextStep(pastBytes ++ upTo0.toArray[Byte], checkCrc16, false, commentsToSkip - 1).feed(rest)
      }
  }

  private class CheckCrc16Step(pastBytes: Array[Byte], pastCrc16Bytes: Array[Byte]) extends State {
    def feed: Chunk[Byte] => (State, Chunk[Byte]) =
      c => {
        val (crc16Bytes, rest) = (pastCrc16Bytes ++ c.toArray[Byte]).splitAt(2)
        if (crc16Bytes.length < 2) {
          (new CheckCrc16Step(pastBytes, crc16Bytes), Chunk.empty)
        } else {
          val crc = new CRC32
          crc.update(pastBytes)
          val computedCrc16 = crc.getValue.toInt & 0xffff
          val expectedCrc   = u16(crc16Bytes(0), crc16Bytes(1))
          if (computedCrc16 != expectedCrc) throw CompressionException("Invalid header CRC16")
          else new Decompress().feed(Chunk.fromArray(rest))
        }
      }
  }
  private class Decompress extends State {

    val crc32: CRC32        = new CRC32()
    val buffer: Array[Byte] = new Array[Byte](bufferSize)

    private[compression] def pullOutput(
      inflater: Inflater,
      buffer: Array[Byte]
    ): Chunk[Byte] = {
      @tailrec
      def next(prev: Chunk[Byte]): Chunk[Byte] = {
        val read     = inflater.inflate(buffer)
        val newBytes = ju.Arrays.copyOf(buffer, read)
        crc32.update(newBytes)
        val current = Chunk.fromArray(newBytes)
        val pulled  = prev ++ current
        if (read > 0 && inflater.getRemaining > 0) next(pulled) else pulled
      }
      if (inflater.needsInput()) Chunk.empty else next(Chunk.empty)
    }

    def feed: Chunk[Byte] => (State, Chunk[Byte]) = chunk => {
      inflater.setInput(chunk.toArray)
      val newChunk = pullOutput(inflater, buffer)
      if (inflater.finished()) {
        val leftover   = chunk.takeRight(inflater.getRemaining())
        val (state, _) = new CheckTrailerStep(Array.empty, crc32.getValue(), inflater.getBytesWritten()).feed(leftover)
        (state, newChunk) // CheckTrailerStep returns empty chunk only
      } else ((this, newChunk))
    }
  }

  private class CheckTrailerStep(oldBytes: Array[Byte], expectedCrc32: Long, expectedIsize: Long) extends State {

    def readInt(a: Array[Byte]): Int = u32(a(0), a(1), a(2), a(3))

    override def feed: Chunk[Byte] => (State, Chunk[Byte]) =
      c => {
        val bytes = oldBytes ++ c.toArray[Byte]
        if (bytes.length < 8) ((this, Chunk.empty)) // need more input
        else {
          reset()
          val (trailerBytes, leftover) = bytes.splitAt(8)
          val crc32                    = readInt(trailerBytes.take(4))
          val isize                    = readInt(trailerBytes.drop(4))
          if (expectedCrc32.toInt != crc32) throw CompressionException("Invalid CRC32")
          else if (expectedIsize.toInt != isize) throw CompressionException("Invalid ISIZE")
          else new ParseHeaderStep(leftover).feed(Chunk.empty)
        }
      }
  }

}

private[compression] object Gunzipper {

  private val fixedHeaderLength = 10

  private sealed trait State {
    def feed: Chunk[Byte] => (State, Chunk[Byte])
  }

  def make(bufferSize: Int): ZIO[Any, Nothing, Gunzipper] = ZIO.succeed(new Gunzipper(bufferSize))
}
