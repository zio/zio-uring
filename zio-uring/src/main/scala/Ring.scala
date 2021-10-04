package zio.uring.native

import zio.Chunk
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.nio.ByteBuffer

class Ring(native: Native, ringFd: Long, completionsChunkSize: Int) {
  val pendingReqs = new ConcurrentHashMap[Long, Callback]
  val requestIds = new AtomicLong(Long.MinValue)

  def close(): Unit =
    native.destroyQueue(ringFd)

  def open(path: String): FileDescriptor =
    FileDescriptor(Ring.native.openFile(path))

  def read(file: FileDescriptor, offset: Long, length: Long, cb: Chunk[Byte] => Unit): Unit = {
    val reqId = requestIds.getAndIncrement()
    val resultBuffer = native.read(ringFd, reqId, file.fd, offset, length)

    pendingReqs.put(reqId, Callback.Read(resultBuffer, cb))
    ()
  }

  def write(file: FileDescriptor, offset: Long, data: Array[Byte], cb: Long => Unit): Unit = {
    val reqId = requestIds.getAndIncrement()
    native.write(ringFd, reqId, file.fd, offset, data)

    pendingReqs.put(reqId, Callback.Write(cb))
    ()
  }

  def submit(): Unit =
    native.submit(ringFd)

  def await(): Unit = {
    val data = native.peek(ringFd, completionsChunkSize)
    println(data.mkString(","))
    val (reqIds, results) = data.splitAt(data.size / 2)

    for ((reqId, result) <- reqIds.zip(results)) {
      pendingReqs.remove(reqId) match {
        case Callback.Read(buf, cb) =>
          buf.limit(result.toInt)
          cb(Chunk.fromByteBuffer(buf))
        case Callback.Write(cb) => cb(result)
        case null => sys.error(s"Oops: nonexistent request $reqId completed")
      }
    }
  }

  def erase[A](cb: A => Unit): Any => Unit =
    cb.asInstanceOf[Any => Unit]
}

object Ring {
  private val native = new Native

  def make(queueSize: Int, completionsChunkSize: Int): Ring =
    new Ring(native, native.initQueue(queueSize), completionsChunkSize)
}


case class FileDescriptor(fd: Int) extends AnyVal

sealed trait Callback
object Callback {
  case class Read(buf: ByteBuffer, cb: Chunk[Byte] => Unit) extends Callback
  case class Write(cb: Long => Unit) extends Callback
}
