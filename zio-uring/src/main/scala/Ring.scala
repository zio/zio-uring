package zio.uring.native

import zio._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean

class Ring(native: Native, ringFd: Long, completionsChunkSize: Int) {
  val pendingReqs = new ConcurrentHashMap[Long, Callback]
  val requestIds  = new AtomicLong(Long.MinValue)

  val shutdown: AtomicBoolean = new AtomicBoolean(false)

  /**
   * Allocate a direct buffer for returning completion queue events. This will be a struct
   * with 3 fields:
   *      1. request ID (Long)
   *      2. return code (Int)
   *      3. flags (Int)
   */
  private val resultBuffer: ByteBuffer =
    ByteBuffer.allocateDirect(completionsChunkSize * 16).order(ByteOrder.BIG_ENDIAN)

  def close(): Unit = {
    shutdown.set(true)
    native.destroyRing(ringFd)
  }

  def open(path: String, cb: Int => Unit): Long = {
    val reqId  = requestIds.getAndIncrement()
    val argPtr = native.openFile(ringFd, reqId, path)
    pendingReqs.put(reqId, Callback.OpenFile(argPtr, cb))
    reqId
  }

  def statx(path: String, cb: Either[Int, StatxBuffer] => Unit): Long = {
    val reqId  = requestIds.getAndIncrement()
    val buffer = ByteBuffer.allocateDirect(256).order(ByteOrder.nativeOrder())
    val argPtr = native.statx(ringFd, reqId, path, buffer)
    pendingReqs.put(reqId, Callback.Statx(argPtr, buffer, cb))
    reqId
  }

  def read(
    file: FileDescriptor,
    offset: Long,
    length: Int,
    ioLinked: Boolean,
    cb: Either[Int, Chunk[Byte]] => Unit
  ): Long = {
    val reqId  = requestIds.getAndIncrement()
    val buffer = ByteBuffer.allocateDirect(length)
    pendingReqs.put(reqId, Callback.Read(buffer, cb))
    native.read(ringFd, reqId, file.fd, offset, buffer, ioLinked)
    reqId
  }

  def write(file: FileDescriptor, offset: Long, data: Array[Byte], ioLinked: Boolean, cb: Int => Unit): Long = {
    val reqId  = requestIds.getAndIncrement()
    val buffer = ByteBuffer.allocateDirect(data.size)
    buffer.put(data)
    pendingReqs.put(reqId, Callback.Write(cb))
    native.write(ringFd, reqId, file.fd, offset, buffer, ioLinked)
    reqId
  }

  def peek(): Unit =
    if (!shutdown.get()) {
      var run = true
      resultBuffer.clear()
      native.peek(ringFd, completionsChunkSize, resultBuffer)
      while (run) {
        val reqId   = resultBuffer.getLong()
        val retCode = resultBuffer.getInt()
        val _       = resultBuffer.getInt()
        pendingReqs.remove(reqId) match {
          case Callback.Read(buf, cb) if retCode >= 0          => cb(Right(Chunk.fromByteBuffer(buf)))
          case Callback.Read(buf, cb) if retCode < 0           => cb(Left(retCode))
          case c @ Callback.Write(cb)                          => cb(retCode)
          case c @ Callback.OpenFile(argPtr, cb)               =>
            native.freeString(argPtr)
            cb(retCode)
          case Callback.Statx(argPtr, buf, cb) if retCode >= 0 =>
            native.freeString(argPtr)
            cb(StructDecoder[StatxBuffer].decode(buf))
          case Callback.Statx(argPtr, _, cb) if retCode < 0    =>
            native.freeString(argPtr)
            cb(Left(retCode))
          // Ignore operations cancelled after completion
          case Callback.Cancelled                              => ()
          // Need a better way to know when we've read everything....
          case null                                            => run = false //sys.error(s"Oops: nonexistent request $reqId completed")
        }
      }
    } else println(s"Ring is already shutdown")

  def submit(): Unit =
    native.submit(ringFd)

  def await(count: Int): Unit = {
    var run = true
    resultBuffer.clear()
    native.await(ringFd, count, resultBuffer)
    while (run) {
      val reqId   = resultBuffer.getLong()
      val retCode = resultBuffer.getInt()
      val _       = resultBuffer.getInt()
      pendingReqs.remove(reqId) match {
        case Callback.Read(buf, cb) if retCode >= 0          => cb(Right(Chunk.fromByteBuffer(buf)))
        case Callback.Read(_, cb) if retCode < 0             => cb(Left(retCode))
        case c @ Callback.Write(cb)                          => cb(retCode)
        case c @ Callback.OpenFile(argPtr, cb)               =>
          native.freeString(argPtr)
          cb(retCode)
        case Callback.Statx(argPtr, buf, cb) if retCode >= 0 =>
          native.freeString(argPtr)
          cb(StructDecoder[StatxBuffer].decode(buf))
        case Callback.Statx(argPtr, _, cb) if retCode < 0    =>
          native.freeString(argPtr)
          cb(Left(retCode))
        // Ignore operations cancelled after completion
        case Callback.Cancelled                              => ()
        // Need a better way to know when we've read everything....
        case null                                            => run = false //sys.error(s"Oops: nonexistent request $reqId completed")
      }
    }
  }

  def cancel(requestId: Long): Unit =
    if (!shutdown.get()) {
      val reqId = requestIds.getAndIncrement()
      // If the request is still pending completion replace the callback with a NOP
      // since we cannot guarantee the cancellation will happen before the existing
      // operation is cancelled
      pendingReqs.replace(requestId, Callback.Cancelled) match {
        case null => ()
        case c    => native.cancel(ringFd, reqId, requestId)
      }
    }

  def erase[A](cb: A => Unit): Any => Unit =
    cb.asInstanceOf[Any => Unit]
}

object Ring {
  private val native = new Native

  def make(queueSize: Int, completionsChunkSize: Int): Ring =
    new Ring(native, native.initRing(queueSize), completionsChunkSize)

}

trait StructDecoder[A] {
  def decode(buffer: ByteBuffer): Either[Int, A]
}

object StructDecoder {
  def apply[A](implicit decoder: StructDecoder[A]): StructDecoder[A] = decoder
}

case class FileDescriptor(fd: Int) extends AnyVal
case class StatxBuffer(
  mask: Int,
  blksize: Int,
  attributes: Long,
  nlink: Int,
  uid: Int,
  gid: Int,
  mode: Short,
  ino: Long,
  size: Long,
  blocks: Long,
  attributesMask: Long,
  atime: (Long, Int),
  btime: (Long, Int),
  ctime: (Long, Int),
  mtime: (Long, Int),
  rdevMajor: Int,
  rdevMinor: Int,
  devMajor: Int,
  devMinor: Int
)
object StatxBuffer {
  implicit val decoder: StructDecoder[StatxBuffer] = new StructDecoder[StatxBuffer] {
    def decode(buffer: ByteBuffer): Either[Int, StatxBuffer] = try {
      val mask       = buffer.getInt()
      val blksize    = buffer.getInt()
      val attributes = buffer.getLong()
      val nlink      = buffer.getInt()
      val uid        = buffer.getInt()
      val gid        = buffer.getInt()
      val mode       = buffer.getShort()
      buffer.getShort() // Padding bytes
      val ino       = buffer.getLong()
      val size      = buffer.getLong()
      val blocks    = buffer.getLong()
      val attrsMask = buffer.getLong()
      val atime     = (buffer.getLong(), buffer.getInt())
      buffer.getInt() // padding bytes
      val btime = (buffer.getLong(), buffer.getInt())
      buffer.getInt() // padding bytes
      val ctime = (buffer.getLong(), buffer.getInt())
      buffer.getInt() // padding bytes
      val mtime = (buffer.getLong(), buffer.getInt())
      buffer.getInt() // padding bytes
      val rdevMajor = buffer.getInt()
      val rdevMinor = buffer.getInt()
      val devMajor  = buffer.getInt()
      val devMinor  = buffer.getInt()
      Right(
        StatxBuffer(
          mask,
          blksize,
          attributes,
          nlink,
          uid,
          gid,
          mode,
          ino,
          size,
          blocks,
          attrsMask,
          atime,
          btime,
          ctime,
          mtime,
          rdevMajor,
          rdevMinor,
          devMajor,
          devMinor
        )
      )
    } catch {
      case t: Throwable =>
        Left(-99)
    }
  }
}

sealed trait Callback
object Callback {
  case class Read(buf: ByteBuffer, cb: Either[Int, Chunk[Byte]] => Unit)                extends Callback
  case class Write(cb: Int => Unit)                                                     extends Callback
  case class OpenFile(argPtr: Long, cb: Int => Unit)                                    extends Callback
  case class Statx(argPtr: Long, buf: ByteBuffer, cb: Either[Int, StatxBuffer] => Unit) extends Callback
  case object Cancelled                                                                 extends Callback
}
