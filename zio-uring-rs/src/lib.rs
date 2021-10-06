use jni::JNIEnv;

use jni::objects::{JObject,JByteBuffer,JString,JValue};

use jni::sys::{jint,jlong,jlongArray};

use std::cell::RefCell;
use std::thread;

// use io_uring::IoUring;

use std::{fs,io};
use std::os::unix::io::AsRawFd;
use io_uring::{opcode, types, IoUring};


thread_local!(static RING: RefCell<IoUring> = RefCell::new(IoUring::new(8).unwrap()));


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
pub extern "system" fn Java_zio_uring_native_Native_peek(env: JNIEnv, _ignore: JObject, ringPtr: jlong, count: jint) -> jlongArray {
    env.new_long_array(0).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_await(env: JNIEnv, _ignore: JObject, ringPtr: jlong, count: jint) -> jlongArray {
    env.new_long_array(0).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_openFile(env: JNIEnv, _ignore: JObject, path: JString) -> jint {
    let fpath: String = env.get_string(path).unwrap().into();
    let fd = fs::File::open(fpath).unwrap();
    fd.as_raw_fd()
}

#[no_mangle]
pub extern "system" fn Java_zio_uring_native_Native_readFile(env: JNIEnv, _ignore: JObject, path: JString, cb: JObject) -> () {
    let CB_OBJECT = env.new_global_ref(cb).unwrap();
    let fpath: String = env.get_string(path).unwrap().into();

    // let mut ring = IoUring::new(8).unwrap();

    let fd = fs::File::open(fpath).unwrap();
    let mut buf = vec![0; 1024];

    let read_e = opcode::Read::new(types::Fd(fd.as_raw_fd()), buf.as_mut_ptr(), buf.len() as _)
        .build()
        .user_data(0x42);

    RING.with(|r| {
        let mut ring = r.borrow_mut();
        unsafe {
            ring.submission()
                .push(&read_e)
                .expect("submission queue is full");
        }
    
        ring.submit_and_wait(1).unwrap();

        let direct_buf = env.new_direct_byte_buffer(&mut buf).unwrap();
        let cb_object = CB_OBJECT.as_obj();
        match env.call_method(cb_object, "readBuffer", "(Ljava/nio/ByteBuffer;)V",&[JValue::from(direct_buf)]) {
            Ok(_) => println!("Success!"),
            Err(e) => eprintln!("Error: {}", e)
        }
    });
}


