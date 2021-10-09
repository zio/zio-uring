package zio.uring

import zio._
import zio.console._
import zio.internal.Platform
import zio.internal.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

object Uring extends App {

  override def platform: Platform = Platform.default.withExecutor(
    Executor.fromThreadPoolExecutor(_ => 24)(Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor])
  )

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    RingIO
      .make(128, 32)
      .toManaged(_.close().orDie)
      .use { ring =>
        for {
          poll  <- ring.poll().forever.forkDaemon
          o     <- ring.open("/etc/passwd").fork
          // _     <- ring.submit()
          fd    <- o.join
          _     <- putStrLn(s"Opened $fd")
          read1 <- ring.read(fd, 0, 1024).fork
          read2 <- ring.read(fd, 0, 1024).fork
          // _     <- ring.submit()
          data1 <- read1.join
          data2 <- read2.join
          _     <- putStrLn(s"Read:\n${new String(data1.toArray[Byte], "UTF-8")}")
          _     <- putStrLn(s"Read:\n${new String(data2.toArray[Byte], "UTF-8")}")
          _     <- poll.interrupt
          _     <- getStrLn
        } yield ()

      }
      .exitCode
}
