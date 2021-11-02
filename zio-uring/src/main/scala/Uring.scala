package zio.uring

import zio._
import java.nio.file.Files

object Uring extends ZIOAppDefault {

  override def run =
    RingIO
      .managed(128, 32)
      .use { ring =>
        for {
          tempFile     <- IO.attempt(Files.createTempFile("temp", ".txt"))
          fd           <- ring.open(tempFile.toString)
          _            <- ring
                            .open("not-a-file")
                            .catchAll(ioe => Console.printError(s"File open failed with message: ${ioe.getMessage()}"))
          _            <- Console.printLine(s"Opened $fd")
          bytesWritten <- ring.write(fd, 0, "foobar".getBytes(), false)
          _            <- Console.printLine(s"Wrote $bytesWritten bytes to $fd")
          read1        <- ring.read(fd, 0, 1024, true).fork
          read2        <- ring.read(fd, 0, 1024, false).fork
          data1        <- read1.join
          data2        <- read2.join
          statx        <- ring.statx(tempFile.toString()).fork
          _            <- ring.submit()
          statxResult  <- statx.join
          _            <- Console.printLine(s"Statx szie: ${statxResult.size}")
          _            <- Console.printLine(s"Read:\n${new String(data1.toArray[Byte], "UTF-8")}")
          _            <- Console.printLine(s"Read:\n${new String(data2.toArray[Byte], "UTF-8")}")
        } yield ()
      }
      .catchAll(e => Console.printError(s"IOException: ${e.getMessage}"))
      .exitCode
}
