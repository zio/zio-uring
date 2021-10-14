package zio.uring

import zio._
import zio.uring.native._
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

class RingIO(uring: Ring) {
  def open(path: String): IO[IOException, FileDescriptor] =
    IO.effectAsyncInterrupt { cb =>
      val reqId = uring.open(
        path,
        result =>
          cb(
            if (result < 0) IO.fail(new IOException(s"Open file at path $path failed with errno $result"))
            else IO.succeed(FileDescriptor(result))
          )
      )
      uring.submit()
      Left(URIO.effectTotal(uring.cancel(reqId)))
    }

  def read(file: FileDescriptor, offset: Long, length: Int, ioLinked: Boolean): IO[IOException, Chunk[Byte]] =
    IO.effectAsyncInterrupt { cb =>
      val reqId = uring.read(
        file,
        offset,
        length,
        ioLinked,
        {
          case Left(errno) => cb(IO.fail(new IOException(s"Read on file $file failed with error $errno")))
          case Right(data) => cb(IO.succeed(data))
        }
      )
      if (!ioLinked) uring.submit()
      Left(URIO.effectTotal(uring.cancel(reqId)))
    }

  def write(file: FileDescriptor, offset: Long, data: Array[Byte], ioLinked: Boolean): IO[IOException, Int] =
    IO.effectAsyncInterrupt { cb =>
      val reqId = uring.write(
        file,
        offset,
        data,
        ioLinked,
        result =>
          cb(
            if (result < 0) IO.fail(new IOException(s"Failed to write to file $file with errno $result"))
            else IO.succeed(result)
          )
      )
      if (!ioLinked) uring.submit()
      Left(URIO.effectTotal(uring.cancel(reqId)))
    }

  def submit(): IO[IOException, Unit] = IO.effectTotal(uring.submit())

  def poll(): IO[IOException, Unit] = IO.effectTotal(uring.peek())

  def close(): IO[IOException, Unit] = IO.succeed(uring.close())
}

object RingIO {
  def make(queueSize: Int, completionsChunkSize: Int, shutdown: AtomicBoolean): IO[IOException, RingIO] = IO.succeed {
    new RingIO(Ring.make(queueSize, completionsChunkSize, shutdown))
  }

  def managed(queueSize: Int, completionsChunkSize: Int): ZManaged[Any, IOException, RingIO] = {
    val shutdown = new AtomicBoolean(false)
    val effect   = for {
      ring   <- make(queueSize, completionsChunkSize, shutdown)
      poller <- ring.poll().repeatUntil(_ => shutdown.get()).fork
    } yield (ring, poller)

    effect.toManaged { case (ring, poller) =>
      UIO(shutdown.set(true)) *>
      poller.await *> ring.close().orDie
    }.map(_._1)
  }
}
