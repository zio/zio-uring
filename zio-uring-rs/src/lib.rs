use jni::JNIEnv;

use jni::objects::{JByteBuffer, JObject, JString};

use jni::sys::{jboolean, jint, jlong};

use io_uring::{opcode, squeue, types, IoUring};
use std::ffi::CString;
use std::mem::transmute;
use std::os::unix::ffi::OsStrExt;
use std::{thread, time};

#[no_mangle]
pub unsafe extern "system" fn Java_zio_uring_native_Native_initRing(
    _env: JNIEnv,
    _ignore: JObject,
    entries: jint,
) -> jlong {
    // let uring = IoUring::builder().setup_iopoll().build(entries as _).unwrap();
    let uring = IoUring::new(entries as _).unwrap();
    Box::into_raw(Box::new(uring)) as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_destroyRing(
    _env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
) -> () {
    let uring = Box::from_raw(ringPtr as *mut IoUring);
    drop(uring);
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_submit(
    _env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
) -> () {
    let uring = &mut *(ringPtr as *mut IoUring);
    uring.submit().unwrap();
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_statx(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    reqId: jlong,
    path: JString,
    buffer: JByteBuffer,
) -> jlong {
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let buf: &mut [u8] = env.get_direct_buffer_address(buffer).unwrap();

    let dirfd = types::Fd(libc::AT_FDCWD);
    let rpath: String = env.get_string(path).unwrap().into();
    println!("Stat'ing file {}", rpath);
    let os_path = std::ffi::OsString::from(rpath);
    let fpath_ptr = CString::new(os_path.as_os_str().as_bytes())
        .unwrap()
        .into_raw();

    let stat_e = opcode::Statx::new(
        dirfd,
        fpath_ptr,
        buf.as_mut_ptr() as *mut libc::statx as *mut _,
    )
    .mask(libc::STATX_ALL)
    .build()
    .user_data(reqId as u64);

    uring.submission().push(&stat_e).expect("queue is full");
    // uring.submit().unwrap();
    fpath_ptr as _
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_read(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    reqId: jlong,
    fd: jint,
    _offset: jlong,
    buffer: JByteBuffer,
    ioLinked: jboolean,
) -> () {
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let buf: &mut [u8] = env.get_direct_buffer_address(buffer).unwrap();
    let length = env.get_direct_buffer_capacity(buffer).unwrap() as u32;
    let read_e = if ioLinked != 0 {
        opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), length)
            .build()
            .user_data(reqId as u64)
    } else {
        opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), length)
            .build()
            .user_data(reqId as u64)
            .flags(squeue::Flags::IO_LINK)
    };

    uring
        .submission()
        .push(&read_e)
        .expect("submission queue is full");
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_write(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    reqId: jlong,
    fd: jint,
    _offset: jlong,
    buffer: JByteBuffer,
    ioLinked: jboolean,
) -> () {
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let buf: &mut [u8] = env.get_direct_buffer_address(buffer).unwrap();
    let length = env.get_direct_buffer_capacity(buffer).unwrap() as u32;
    let write_e = if ioLinked != 0 {
        opcode::Write::new(types::Fd(fd), buf.as_mut_ptr(), length)
            .build()
            .user_data(reqId as u64)
    } else {
        opcode::Write::new(types::Fd(fd), buf.as_mut_ptr(), length)
            .build()
            .user_data(reqId as u64)
            .flags(squeue::Flags::IO_LINK)
    };

    uring
        .submission()
        .push(&write_e)
        .expect("submission queue is full");
}

#[no_mangle]
#[allow(non_snake_case)]
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
    cq.sync();
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_await(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    count: jint,
    buffer: JByteBuffer,
) -> () {
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let poll_interval = time::Duration::from_micros(50);

    let mut count_inner = 0;
    let mut cq = uring.completion();

    let buf: &mut [u8] = env.get_direct_buffer_address(buffer).unwrap();
    let mut buf_offset = 0 as usize;

    while count_inner < count {
        match cq.next() {
            Some(cqe) => {
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
    cq.sync();
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_openFile(
    env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    reqId: jlong,
    path: JString,
) -> jlong {
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let dirfd = types::Fd(libc::AT_FDCWD);
    let rpath: String = env.get_string(path).unwrap().into();
    let os_path = std::ffi::OsString::from(rpath);
    let fpath_ptr = CString::new(os_path.as_os_str().as_bytes())
        .unwrap()
        .into_raw();
    let openhow = types::OpenHow::new().flags(libc::O_RDWR as _);
    // TODO this needs to be passed back to the JVM so it can be freed!
    let openhow_ptr = Box::into_raw(openhow.into());
    let open_e = opcode::OpenAt2::new(dirfd, fpath_ptr, openhow_ptr)
        .build()
        .user_data(reqId as u64);

    uring.submission().push(&open_e).expect("queue is full");

    fpath_ptr as _
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_cancel(
    _env: JNIEnv,
    _ignore: JObject,
    ringPtr: jlong,
    reqId: jlong,
    opReqId: jlong,
) -> () {
    let uring: &mut IoUring = &mut *(ringPtr as *mut IoUring);

    let cancel_e = opcode::AsyncCancel::new(opReqId as u64)
        .build()
        .user_data(reqId as u64);
    uring.submission().push(&cancel_e).expect("queue is full");
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_freeString(
    _env: JNIEnv,
    _ignore: JObject,
    ptr: jlong,
) -> () {
    // retake pointer to free memory
    let _ = CString::from_raw(ptr as _);
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_zio_uring_native_Native_free(
    _env: JNIEnv,
    _ignore: JObject,
    ptr: jlong,
    len: jlong
) -> () {
    // retake pointer to free memory
    let slice = std::slice::from_raw_parts_mut(ptr as *mut u8, len as usize);
    let _ = Box::from_raw(slice as *mut [u8]);
}


