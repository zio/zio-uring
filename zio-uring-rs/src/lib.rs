use jni::JNIEnv;

use jni::objects::{JClass, JObject,JByteBuffer};

use jni::sys::{jstring,jint,jlong,jlongArray};

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_initQueue(env: JNIEnv, _ignore: JObject,depth: jint) -> jlong {
    0 as jlong
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_destroyQueue(env: JNIEnv, _ignore: JObject, ringPtr: jlong) -> () {
    ()
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_submit(env: JNIEnv, _ignore: JObject, ringPtr: jlong) -> () {
    ()
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_read<'a>(env: JNIEnv<'a>, _ignore: JObject, ringPtr: jlong, reqId: jlong, fd: jint, offset: jlong, length: jlong) -> JByteBuffer<'a> {
    let mut buf = vec![0; 1024];
    env.new_direct_byte_buffer(&mut buf).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_peek(env: JNIEnv, _ignore: JObject, ringPtr: jlong, cout: jint) -> jlongArray {
    env.new_long_array(0).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_await(env: JNIEnv, _ignore: JObject, ringPtr: jlong, cout: jint) -> jlongArray {
    env.new_long_array(0).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_openFile(env: JNIEnv, _ignore: JObject, patht: jint) -> jint {
    0
}


