use crate::{ByteArrayRef, ByteArray};

/// Function pointer for RPC callbacks delivering a `(success, failure)`
/// pair as non-owning `ByteArrayRef`s to Rust.
pub(crate) type WorkerClientCallbackTrampoline = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success:   ByteArrayRef,
    failure:   ByteArrayRef,
);

/// Interop-triggered callback trampoline.
///
/// When an interop caller completes an RPC, it invokes this function
/// with two non-owning `ByteArrayRef`s (`success_ref` and `failure_ref`) plus
/// a raw `callback_ud` token that was originally created as
/// `Box::into_raw(Box::new(tx))`.
///
/// This shim:
/// 1. Reclaims the boxed `oneshot::Sender<(ByteArray, ByteArray)>` from `callback_ud`.
/// 2. Deep-copies each `ByteArrayRef` into an owned `ByteArray`.
/// 3. Sends the `(success, failure)` pair through the channel, waking the awaiting task.
///
/// # Safety
/// - `callback_ud` must come from `Box::into_raw(Box::new(tx))`.
/// - `success_ref.data` and `failure_ref.data` must point to `size` valid bytes.
pub(crate) unsafe extern "C" fn worker_client_callback_trampoline(
    callback_ud:  *mut libc::c_void,
    success_ref:  ByteArrayRef,
    failure_ref:  ByteArrayRef,
) {
    // Recover our oneshot sender
    let tx: Box<tokio::sync::oneshot::Sender<(ByteArray, ByteArray)>> =
        unsafe { Box::from_raw(callback_ud as *mut _) };

    // Deep‐copy a ByteArrayRef into an owning ByteArray
    fn into_owned(r: ByteArrayRef) -> ByteArray {
        if r.data.is_null() {       // must not check for size == 0 here, there could be empty, valid responses from Temporal
            // no data → empty, non‐freeing ByteArray
            ByteArray {
                data: std::ptr::null(),
                size: 0,
                cap: 0,
                disable_free: true,     // disable free as otherwise leads to double-free from Oneshot Channel and drop after `worker_client_callback_trampoline`
            }
        } else {
            // SAFETY: the interop side promises `r.data` is valid for `r.size` bytes
            let slice: &[u8] = unsafe { std::slice::from_raw_parts(r.data, r.size) };
            let vec = slice.to_vec(); // deep‐copy
            ByteArray::from_vec_disable_free(vec)   // disable free as otherwise leads to double-free from Oneshot Channel and drop after `worker_client_callback_trampoline`
        }
    }

    let succ = into_owned(success_ref);
    let fail = into_owned(failure_ref);

    // Send the pair back down the channel
    let _ = tx.send((succ, fail));
}