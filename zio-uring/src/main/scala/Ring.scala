package zio.uring.native

import zio._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.nio.ByteBuffer
import java.nio.ByteOrder

class Ring(native: Native, ringFd: Long, completionsChunkSize: Int) {
  val pendingReqs = new ConcurrentHashMap[Long, Callback]
  val requestIds  = new AtomicLong(Long.MinValue)

  private val resultBuffer: ByteBuffer = ByteBuffer.allocateDirect(1024).order(ByteOrder.BIG_ENDIAN)

  def close(): Unit =
    native.destroyRing(ringFd)

  def open(path: String, cb: Int => Unit): Unit = {
    val reqId = requestIds.getAndIncrement()
    pendingReqs.put(reqId, Callback.OpenFile(cb))
    native.openFile(ringFd, reqId, path)
  }

  def read(file: FileDescriptor, offset: Long, length: Int, cb: Chunk[Byte] => Unit): Unit = {
    val reqId  = requestIds.getAndIncrement()
    val buffer = ByteBuffer.allocateDirect(length)
    pendingReqs.put(reqId, Callback.Read(buffer, cb))
    native.read(ringFd, reqId, file.fd, offset, buffer)

    ()
  }

  def write(file: FileDescriptor, offset: Long, data: Array[Byte], cb: Long => Unit): Unit = {
    val reqId = requestIds.getAndIncrement()
    val buffer = ByteBuffer.allocateDirect(data.size)
    buffer.put(data)
    pendingReqs.put(reqId, Callback.Write(cb))
    native.write(ringFd, reqId, file.fd, offset, buffer)
  }

  def peek(): Unit = {
    var run = true
    resultBuffer.clear()
    native.peek(ringFd, completionsChunkSize, resultBuffer)
    while (run) {
      val reqId   = resultBuffer.getLong()
      val retCode = resultBuffer.getInt()
      val _   = resultBuffer.getInt()
      pendingReqs.remove(reqId) match {
        case Callback.Read(buf, cb)    => cb(Chunk.fromByteBuffer(buf))
        case c @ Callback.Write(cb)    => cb(c.readWritten(resultBuffer))
        case c @ Callback.OpenFile(cb) => cb(retCode)
        // Need a better way to know when we've read everything....
        case null                      => run = false //sys.error(s"Oops: nonexistent request $reqId completed")
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
      val _   = resultBuffer.getInt()
      pendingReqs.remove(reqId) match {
        case Callback.Read(buf, cb)    => cb(Chunk.fromByteBuffer(buf))
        case c @ Callback.Write(cb)    => cb(c.readWritten(resultBuffer))
        case c @ Callback.OpenFile(cb) => cb(retCode)
        // Need a better way to know when we've read everything....
        case null                      => run = false //sys.error(s"Oops: nonexistent request $reqId completed")
      }
    }
  }

  def erase[A](cb: A => Unit): Any => Unit =
    cb.asInstanceOf[Any => Unit]
}

object Ring {
  private val native = new Native

  def make(queueSize: Int, completionsChunkSize: Int): Ring = {
    val uring      = new Ring(native, native.initRing(queueSize), completionsChunkSize)
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
  case class Read(buf: ByteBuffer, cb: Chunk[Byte] => Unit) extends Callback
  case class Write(cb: Long => Unit)                        extends Callback {
    def readWritten(buf: ByteBuffer): Long = buf.getLong
  }
  case class OpenFile(cb: Int => Unit)                      extends Callback {
    def readFd(buf: ByteBuffer): Int = buf.getInt
  }
}
