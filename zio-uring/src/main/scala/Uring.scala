package zio.uring

import zio._
import zio.uring.native.Ring

object Uring extends App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    Task(Ring.make(128, 32)).toManaged(ring => Task(ring.close()).orDie).use { ring =>
      Task(ring.open("/etc/passwd", fd => println(s"Opened fd $fd"))).flatMap { fd =>
        Task {
          ring.read(fd, 0, 1024, chunk => println(new String(chunk.toArray[Byte], "UTF-8")))
          ring.read(fd, 0, 1024, chunk => println(new String(chunk.toArray[Byte], "UTF-8")))
          ring.submit()
          ring.await(2)
        }
      }
    }.exitCode
}
