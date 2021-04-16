#include "include/zio_uring_native_Native.h"
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

JNIEXPORT jobject JNICALL Java_zio_uring_native_Native_read(JNIEnv *jni, jobject _ignore, jlong ringPtr, jlong reqId, jint fd, jlong offset, jlong length) {
  void *buf;
  posix_memalign(&buf, length, length);

  jobject destByteBuffer = (*jni)->NewDirectByteBuffer(jni, buf, length);

  struct iovec ios[1];
  ios[0].iov_len = length;
  ios[0].iov_base = buf;
  
  struct io_uring *ring = (struct io_uring *) ringPtr;
  struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

  io_uring_prep_readv(sqe, fd, ios, 1, offset);
  sqe->user_data = reqId;
  io_uring_submit(ring);

  return destByteBuffer;
}

JNIEXPORT jlongArray JNICALL Java_zio_uring_native_Native_await(JNIEnv *jni, jobject _ignore, jlong ringPtr) {
  struct io_uring *ring = (struct io_uring *) ringPtr;
  struct io_uring_cqe *cqes;

  unsigned completions = io_uring_peek_batch_cqe(ring, &cqes, 16);
  
  jlongArray retArray = (*jni)->NewLongArray(jni, completions * 2);
  long *arrayData = malloc(sizeof(long) * completions * 2);

  for (unsigned i = 0; i < completions; i++) {
    arrayData[i] = cqes[i].user_data;
    arrayData[i + completions] = cqes[i].res;
  }

  fflush(stdout);

  (*jni)->SetLongArrayRegion(jni, retArray, 0, completions * 2, arrayData);

  free(arrayData);

  return retArray;
}


JNIEXPORT jint JNICALL Java_zio_uring_native_Native_openFile(JNIEnv *jni, jobject _ignore, jstring path) {
  const char *filename = (*jni)->GetStringUTFChars(jni, path, NULL);

  int fd = open(filename, O_RDONLY);

  (*jni)->ReleaseStringUTFChars(jni, path, filename);

  return fd;
}
