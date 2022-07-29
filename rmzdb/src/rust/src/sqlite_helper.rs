
use anyhow::*;
use rusqlite::Connection;

pub unsafe fn _connection_to_dbh(db: Connection) -> Result<*mut rusqlite::ffi::sqlite3> {

    // Retrieve direct SQLite connection handle
    let dbh: *mut rusqlite::ffi::sqlite3 = db.handle();

    // Don't let Rust drop the DB automatically
    std::mem::forget(db);

    Ok(dbh)
}

pub unsafe fn _cast_connection(dbh_address: i64) -> rusqlite::Result<Connection> {
    let dbh = std::mem::transmute::<i64, *mut rusqlite::ffi::sqlite3>(dbh_address);

    Connection::from_handle(dbh)
}

use std::ffi::CStr;
use std::os::raw::c_char;

pub unsafe fn _sqlite_errmsg_to_string(errmsg: *const c_char) -> String {
    let c_slice = CStr::from_ptr(errmsg).to_bytes();
    String::from_utf8_lossy(c_slice).into_owned()
}

/*
#[cold]
pub fn _sqlite_error_from_sqlite_code(code: c_int, message: Option<String>) -> Error {
    Error::SqliteFailure(rusqlite::ffi::Error::new(code), message)
}*/

// Copied from rusqlite
#[cold]
pub unsafe fn _sqlite_errmsg_from_handle(db: *mut rusqlite::ffi::sqlite3) -> Option<String> {

    let message = if db.is_null() {
        None
    } else {
        Some(_sqlite_errmsg_to_string(rusqlite::ffi::sqlite3_errmsg(db)))
    };

    message
}