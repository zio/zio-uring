package zio.uring

import java.nio.ByteBuffer
import zio._
import zio.duration._
import zio.clock.Clock
import java.util.concurrent.atomic.AtomicBoolean

case class Block(index: Int, buffer: ByteBuffer, size: Int)

private[uring] class Slab(private val buffer: Array[(AtomicBoolean, ByteBuffer)], blockSize: Int) {

  def allocate(timeout: Duration): ZIO[Any with Clock, Nothing, Option[Block]] =
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
    }.repeatUntil(_ >= 0).map(idx => Block(idx, buffer(idx)._2, blockSize)).timeout(timeout)

  def free(block: Block): ZIO[Any with Clock, Nothing, Unit] =
    UIO {
      block.buffer.clear()
      buffer(block.index)._1.set(true)
    }

}

object Slab {
  private[uring] def make(blocks: Int, blockSize: Int = 2048): ZManaged[Any with Clock, Nothing, Slab] =
    for {
      buffer <- UIO {
                  val buffer = Array.ofDim[(AtomicBoolean, ByteBuffer)](blocks)
                  for (idx <- 0 until blocks)
                    buffer(idx) = (new AtomicBoolean(true), ByteBuffer.allocateDirect(blockSize))
                  buffer
                }.toManaged_
    } yield new Slab(buffer, blockSize)
}
