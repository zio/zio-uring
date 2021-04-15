#include <zio_uring_native_Native.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <liburing.h>
#include <sys/uio.h>

JNIEXPORT jlong JNICALL Java_zio_uring_native_Native_initQueue(JNIEnv *jni, jobject _ignore, jint depth) {
  struct io_uring *ring;

  ring = malloc(sizeof(struct io_uring));

  int ret = io_uring_queue_init(depth, ring, 0);
  if (ret < 0)
    perror("initQueue");

  return (long) ring;
}

JNIEXPORT void JNICALL Java_zio_uring_native_Native_destroyQueue(JNIEnv *jni, jobject _ignore, jlong ringPtr) {
  struct io_uring *ring = (struct io_uring*) ringPtr;

  io_uring_queue_exit(ring);

  free(ring);
}

JNIEXPORT jbyteArray JNICALL Java_zio_uring_native_Native_readChunk(JNIEnv *jni, jobject _ignore, jlong ringPtr, jint fd, jlong offset, jlong length) {
  struct iovec ios[1];

  void *buf;
  posix_memalign(&buf, length, length);

  ios[0].iov_len = length;
  ios[0].iov_base = buf;
  
  struct io_uring *ring = (struct io_uring *) ringPtr;
  struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

  io_uring_prep_readv(sqe, fd, ios, 1, offset);
  io_uring_submit(ring);

  struct io_uring_cqe *cqe;
  io_uring_wait_cqe(ring, &cqe);

  jsize retDataLen;
  if (cqe->res < 0)
    retDataLen = 0;
  else
    retDataLen = cqe->res;
  
  jbyteArray retData = (*jni)->NewByteArray(jni, retDataLen);
  (*jni)->SetByteArrayRegion(jni, retData, 0, retDataLen, ios[0].iov_base);

  io_uring_cqe_seen(ring, cqe);
  free(ios[0].iov_base);

  return retData;
}

JNIEXPORT jint JNICALL Java_zio_uring_native_Native_openFile(JNIEnv *jni, jobject _ignore, jstring path) {
  const char *filename = (*jni)->GetStringUTFChars(jni, path, NULL);

  int fd = open(filename, O_RDONLY);

  (*jni)->ReleaseStringUTFChars(jni, path, filename);

  return fd;
}
