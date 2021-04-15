package zio.uring.native

import ch.jodersky.jni.nativeLoader

@nativeLoader("ziouring0")
class Native {
  @native def initQueue(depth: Int): Long

  @native def destroyQueue(queue: Long): Unit

  @native def readChunk(queue: Long, fd: Int, offset: Long, length: Long): Array[Byte]

  @native def openFile(path: String): Int
}
