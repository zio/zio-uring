package zio.uring

import java.nio.ByteBuffer
import zio._
import java.util.concurrent.atomic.AtomicBoolean
import zio.uring.native.Native

case class Block(index: Int, bufferAddress: Long, buffer: ByteBuffer, size: Int)

private[uring] class Slab(
  buffer: Array[(AtomicBoolean, ByteBuffer)],
  native: Native,
  val blocks: Int,
  val blockSize: Int
) {

  def allocate(): UIO[Block] =
    UIO {
      var (idx, loop) = (0, true)
      while (loop)
        if (idx == buffer.length - 1) {
          idx = -1
          loop = false
        } else if (buffer(idx)._1.compareAndSet(true, false)) {
          loop = false
        } else {
          idx += 1
        }
      idx
    }.repeatUntil(_ >= 0)
      .map(idx => Block(idx, native.byteBufferAddress(buffer(idx)._2), buffer(idx)._2, blockSize))

  def free(block: Block): UIO[Unit] =
    UIO {
      block.buffer.clear()
      buffer(block.index)._1.set(true)
    }

}

object Slab {
  def make(blocks: Int, blockSize: Int = 2048): Slab = {

    val buffer = Array.ofDim[(AtomicBoolean, ByteBuffer)](blocks)
    for (idx <- 0 until blocks)
      buffer(idx) = (new AtomicBoolean(true), ByteBuffer.allocateDirect(blockSize))

    new Slab(buffer, new Native(), blocks, blockSize)
  }
}
