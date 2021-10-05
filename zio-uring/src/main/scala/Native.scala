package zio.uring.native

import com.github.sbt.jni.nativeLoader
import java.nio.ByteBuffer

@nativeLoader("ziouring0")
class Native {
  @native def initQueue(depth: Int): Long

  @native def destroyQueue(queue: Long): Unit

  @native def read(queue: Long, requestId: Long, fd: Int, offset: Long, length: Long): ByteBuffer

  @native def write(queue: Long, requestId: Long, fd: Int, offset: Long, data: Array[Byte]): Unit

  @native def submit(queue: Long): Unit

  @native def peek(queue: Long, count: Int): Array[Long]

  @native def await(queue: Long, count: Int): Array[Long]

  @native def openFile(path: String): Int
}
