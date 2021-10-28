package zio.uring

import zio._
import zio.console._
import java.nio.file.Files

object Uring extends App {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    RingIO
      .managed(128, 32)
      .use { ring =>
        for {
          tempFile     <- IO.effectTotal(Files.createTempFile("temp", ".txt"))
          fd           <- ring.open(tempFile.toString)
          _            <- ring.open("not-a-file").catchAll(ioe => putStrLn(s"File open failed with message: ${ioe.getMessage()}"))
          _            <- putStrLn(s"Opened $fd")
          bytesWritten <- ring.write(fd, 0, "foobar".getBytes(), false)
          _            <- putStrLn(s"Wrote $bytesWritten bytes to $fd")
          read1        <- ring.read(fd, 0, 1024, true).fork
          read2        <- ring.read(fd, 0, 1024, false).fork
          data1        <- read1.join
          data2        <- read2.join
          statx        <- ring.statx(tempFile.toString()).fork
          // statx        <- ring.statx("/home/ubuntu/source/zio-uring/README.md").fork
          _            <- ring.submit()
          statxResult  <- statx.join
          _            <- putStrLn(s"Statx szie: ${statxResult.size}")
          _            <- putStrLn(s"Read:\n${new String(data1.toArray[Byte], "UTF-8")}")
          _            <- putStrLn(s"Read:\n${new String(data2.toArray[Byte], "UTF-8")}")
        } yield ()
      }
      .catchAll(e => putStrLn(s"IOException: ${e.getMessage}"))
      .exitCode
}
