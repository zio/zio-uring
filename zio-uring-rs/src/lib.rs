use jni::JNIEnv;

use jni::objects::{JByteBuffer, JObject, JString};

use jni::sys::{jint, jlong};

use io_uring::{opcode, types, IoUring};
use std::ffi::CString;
use std::mem::transmute;
use std::os::unix::ffi::OsStrExt;
use std::{thread, time};

#[no_mangle]
pub unsafe extern "system" fn Java_zio_uring_native_Native_initRing(
    env: JNIEnv,
    _ignore: JObject,
    entries: jint,
) -> jlong {
    let uring = IoUring::new(entries as _).unwrap();
    Box::into_raw(Box::new(uring)) as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_zio_uring_native_Native_destroyRing(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
) -> () {
    let _uring = Box::from_raw(ringPtr as *mut IoUring);
}

#[no_mangle]
pub unsafe extern "system" fn Java_zio_uring_native_Native_submit(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
) -> () {
    let uring = &mut *(ringPtr as *mut IoUring);
    let submitted = uring.submit().unwrap();
    println!("Submitted {} events on ring {}", submitted, ringPtr);
}

#[no_mangle]
pub unsafe extern "system" fn Java_zio_uring_native_Native_read(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    reqId: jlong,
    fd: jint,
    offset: jlong,
    buffer: JByteBuffer,
) -> () {
    println!("Reading fd {} on ring {}", fd, ringPtr);
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let buf: &mut [u8] = env.get_direct_buffer_address(buffer).unwrap();
    let length = env.get_direct_buffer_capacity(buffer).unwrap() as u32;
    let read_e = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), length)
        .build()
        .user_data(reqId as u64);

    uring
        .submission()
        .push(&read_e)
        .expect("submission queue is full");
}

#[no_mangle]
pub unsafe extern "system" fn Java_zio_uring_native_Native_peek(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    count: jint,
    buffer: JByteBuffer,
) -> () {
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let mut count_inner = 0;
    let mut cq = uring.completion();

    let buf: &mut [u8] = env.get_direct_buffer_address(buffer).unwrap();
    let mut buf_offset = 0 as usize;

    while count_inner < count && !cq.is_empty() {
        let cqe = cq.next().unwrap();
        cq.sync();
        println!(
            "Found completion {}, {}, {}",
            cqe.user_data(),
            cqe.result(),
            cqe.flags()
        );
        let ud_as_bytes: [u8; 8] = transmute(cqe.user_data().to_be());
        for b in ud_as_bytes {
            buf[buf_offset] = b;
            buf_offset += 1;
        }

        let result_as_bytes: [u8; 4] = transmute(cqe.result().to_be());
        for b in result_as_bytes {
            buf[buf_offset] = b;
            buf_offset += 1;
        }

        let flags_as_bytes: [u8; 4] = transmute(cqe.flags().to_be());
        for b in flags_as_bytes {
            buf[buf_offset] = b;
            buf_offset += 1;
        }
        count_inner += 1;
    }
}

#[no_mangle]
pub unsafe extern "system" fn Java_zio_uring_native_Native_await(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    count: jint,
    buffer: JByteBuffer,
) -> () {
    println!("Awaiting {} completions on ring {}", count, ringPtr);
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let poll_interval = time::Duration::from_micros(50);

    let mut count_inner = 0;
    let mut cq = uring.completion();

    let buf: &mut [u8] = env.get_direct_buffer_address(buffer).unwrap();
    let mut buf_offset = 0 as usize;

    while count_inner < count {
        match cq.next() {
            Some(cqe) => {
                println!("Found completions {}", cqe.user_data());
                let ud_as_bytes: [u8; 8] = transmute(cqe.user_data().to_be());
                for b in ud_as_bytes {
                    buf[buf_offset] = b;
                    buf_offset += 1;
                }

                let result_as_bytes: [u8; 4] = transmute(cqe.result().to_be());
                for b in result_as_bytes {
                    buf[buf_offset] = b;
                    buf_offset += 1;
                }
                let flags_as_bytes: [u8; 4] = transmute(cqe.flags().to_be());
                for b in flags_as_bytes {
                    buf[buf_offset] = b;
                    buf_offset += 1;
                }
                count_inner += 1;
            }
            None => thread::sleep(poll_interval),
        };
    }
}

#[no_mangle]
pub unsafe extern "system" fn Java_zio_uring_native_Native_openFile(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    reqId: jlong,
    path: JString,
) -> () {
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let dirfd = types::Fd(libc::AT_FDCWD);
    let rpath: String = env.get_string(path).unwrap().into();
    let os_path = std::ffi::OsString::from(rpath);
    let fpath = CString::new(os_path.as_os_str().as_bytes()).unwrap();
    let openhow = types::OpenHow::new().flags(libc::O_CREAT as _);
    let open_e = opcode::OpenAt2::new(dirfd, fpath.as_ptr(), &openhow);

    uring
        .submission()
        .push(&open_e.build().user_data(reqId as u64))
        .expect("queue is full");
    // For some reason, if we don't submit this now, then we get an EINVAL error code.
    uring.submit().unwrap();
}

