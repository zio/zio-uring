package zio.uring

import zio._
import java.io.IOException
import zio.stream.ZStream

class ZUring(ring: RingIO) {

    def readFile(path: String): IO[IOException, Array[Byte]] = 
        for {
            stats <- ring.statx(path)
            fd <- ring.open(path)
            data <- ring.read(fd, 0, stats.size.toInt, false)
        } yield data.toArray

    def writeFile(path: String, writeMode: String, data: Array[Byte]): IO[IOException, Nothing] = ???

    def connect(ip: String, port: Int): ZStream[Any, IOException, Array[Byte]] = ???

    def listen(addr: String, protocol: String): ZStream[Any, IOException, Array[Byte]] = ???
  
}
