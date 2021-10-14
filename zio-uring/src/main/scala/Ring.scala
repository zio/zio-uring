package zio.uring.native

import zio._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean

class Ring(native: Native, ringFd: Long, completionsChunkSize: Int, shutdown: AtomicBoolean) {
  val pendingReqs = new ConcurrentHashMap[Long, Callback]
  val requestIds  = new AtomicLong(Long.MinValue)

  private val resultBuffer: ByteBuffer = ByteBuffer.allocateDirect(1024).order(ByteOrder.BIG_ENDIAN)

  def close(): Unit =
    native.destroyRing(ringFd)

  def open(path: String, cb: Int => Unit): Long = {
    val reqId = requestIds.getAndIncrement()
    pendingReqs.put(reqId, Callback.OpenFile(cb))
    native.openFile(ringFd, reqId, path)
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
          case Callback.Read(buf, cb) if retCode >= 0 => cb(Right(Chunk.fromByteBuffer(buf)))
          case Callback.Read(buf, cb) if retCode < 0  => cb(Left(retCode))
          case c @ Callback.Write(cb)                 => cb(retCode)
          case c @ Callback.OpenFile(cb)              => cb(retCode)
          // Need a better way to know when we've read everything....
          case null                                   => run = false //sys.error(s"Oops: nonexistent request $reqId completed")
        }
      }
    }

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
        case Callback.Read(buf, cb) if retCode >= 0 => cb(Right(Chunk.fromByteBuffer(buf)))
        case Callback.Read(buf, cb) if retCode < 0  => cb(Left(retCode))
        case c @ Callback.Write(cb)                 => cb(retCode)
        case c @ Callback.OpenFile(cb)              => cb(retCode)
        // Need a better way to know when we've read everything....
        case null                                   => run = false //sys.error(s"Oops: nonexistent request $reqId completed")
      }
    }
  }

  def cancel(requestId: Long): Unit = {
    val reqId = requestIds.getAndIncrement()
    pendingReqs.remove(requestId) match {
      case null => ()
      case c    => native.cancel(ringFd, reqId, requestId)
    }
  }

  def erase[A](cb: A => Unit): Any => Unit =
    cb.asInstanceOf[Any => Unit]
}

object Ring {
  private val native = new Native

  def make(queueSize: Int, completionsChunkSize: Int, shutdown: AtomicBoolean): Ring = {
    val uring      = new Ring(native, native.initRing(queueSize), completionsChunkSize, shutdown)
    val pollThread = new Thread(() =>
      while (true)
        uring.peek()
    )
    pollThread.setDaemon(true)
    pollThread.start()
    uring
  }
}

case class FileDescriptor(fd: Int) extends AnyVal

sealed trait Callback
object Callback {
  case class Read(buf: ByteBuffer, cb: Either[Int, Chunk[Byte]] => Unit) extends Callback
  case class Write(cb: Int => Unit)                                      extends Callback
  case class OpenFile(cb: Int => Unit)                                   extends Callback
}
