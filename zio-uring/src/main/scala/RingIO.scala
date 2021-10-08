package zio.uring

import zio._
import zio.uring.native._
import java.io.IOException

class RingIO(uring: Ring) {
  def open(path: String): IO[IOException, FileDescriptor] =
    IO.effectAsync { cb =>
      println(s"Opening file at path $path")
      uring.open(path, fd => cb(IO.succeed(FileDescriptor(fd))))
      uring.submit()
    }

  def read(file: FileDescriptor, offset: Long, length: Int): IO[IOException, Chunk[Byte]] =
    IO.effectAsync { cb =>
      println(s"Reading $file")
      uring.read(file, offset, length, data => cb(IO.succeed(data)))
      uring.submit()
    }

  def submit(): IO[IOException, Unit] = IO.effectTotal(uring.submit())

  def poll(): IO[IOException, Unit] = IO.effectTotal(uring.peek())

  def close(): IO[IOException, Unit] = IO.succeed(uring.close())
}

object RingIO {
  def make(queueSize: Int, completionsChunkSize: Int): IO[IOException, RingIO] = IO.succeed {
    new RingIO(Ring.make(queueSize, completionsChunkSize))
  }
}
