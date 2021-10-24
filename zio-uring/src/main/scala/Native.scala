package zio.uring.native

import com.github.sbt.jni.nativeLoader
import java.nio.ByteBuffer

@nativeLoader("ziouring")
class Native {
  @native def initRing(entries: Int): Long

  @native def destroyRing(ringPtr: Long): Unit

  @native def read(ringPtr: Long, reqId: Long, fd: Int, offset: Long, buffer: ByteBuffer, ioLinked: Boolean): Unit

  @native def write(ringPtr: Long, reqId: Long, fd: Int, offset: Long, data: ByteBuffer, ioLinked: Boolean): Unit

  @native def submit(ringPtr: Long): Unit

  @native def peek(ringPtr: Long, count: Int, buffer: ByteBuffer): Unit

  @native def await(ringPtr: Long, count: Int, buffer: ByteBuffer): Unit

  @native def openFile(ringPtr: Long, reqId: Long, path: String): Unit

  @native def cancel(ringPtr: Long, reqId: Long, opReqId: Long): Unit

  @native def send(ringPtr: Long, reqId: Long, socketFd: Int, data: ByteBuffer): Unit

  @native def receive(ringPtr: Long, reqId: Long, socketFd: Int, buffer: ByteBuffer): Unit

}
