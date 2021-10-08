package zio.uring.native

import com.github.sbt.jni.nativeLoader
import java.nio.ByteBuffer

@nativeLoader("ziouring")
class Native {
  @native def initRing(entries: Int): Long

  @native def destroyRing(ringPtr: Long): Unit

  @native def read(ringPtr: Long, reqId: Long, fd: Int, offset: Long, buffer: ByteBuffer): Unit

  @native def write(ringPtr: Long, reqId: Long, fd: Int, offset: Long, data: Array[Byte]): Unit

  @native def submit(ringPtr: Long): Unit

  @native def peek(ringPtr: Long, count: Int, buffer: ByteBuffer): Unit

  @native def await(ringPtr: Long, count: Int, buffer: ByteBuffer): Unit

  @native def openFile(ringPtr: Long, reqId: Long, path: String): Int

  @native def readFile(ringPtr: Long, path: String, buffer: ByteBuffer): Unit
}

trait NativeCallback {
  def readBuffer(buf: ByteBuffer): Unit
}
object NativeCallback {
  def apply(cb: ByteBuffer => Unit): NativeCallback = new NativeCallback {
    def readBuffer(buf: ByteBuffer): Unit = cb(buf)
  }
}
