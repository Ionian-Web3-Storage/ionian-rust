#![allow(dead_code)]

use jsonrpsee::core::Error;
use jsonrpsee::types::error::{CallError, ErrorCode, ErrorObject};

pub fn not_supported() -> Error {
    Error::Call(CallError::Custom(ErrorObject::borrowed(
        ErrorCode::MethodNotFound.code(),
        &"Not supported",
        None,
    )))
}

pub fn internal_error(msg: &'static impl std::convert::AsRef<str>) -> Error {
    Error::Call(CallError::Custom(ErrorObject::borrowed(
        ErrorCode::InternalError.code(),
        msg,
        None,
    )))
}
