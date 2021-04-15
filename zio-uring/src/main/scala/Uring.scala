package zio.uring

import zio._
import zio.uring.native.Native

object Uring extends App {
  override def run(args: List[String]): URIO[ZEnv,ExitCode] = Task {
    val native = new Native
    val ring = native.initQueue(1);
    val fd = native.openFile("/etc/passwd");

    val data = native.readChunk(ring, fd, 0, 64);

    println(new String(data, "UTF-8"));

    native.destroyQueue(ring);
  }.exitCode
 
}
