//! Conversion of data structures to and from MDB_val
//!
//! Since MDB_val is valid through whole transaction, it is kind of safe
//! to keep plain data, i.e. to keep raw pointers and transmute them back
//! and forward into corresponding data structures to avoid any unnecessary
//! copying.
//!
//! `MdbValue` is a simple wrapper with bounded lifetime which should help
//! keep it sane, i.e. provide compile errors when data retrieved outlives
//! transaction.
//!
//! It would be extremely helpful to create `compile-fail` tests to ensure
//! this, but unfortunately there is no way yet.


use std::{self, mem, slice};

use crate::MdbValue;
use ffi::MDB_val;

/// `ToMdbValue` is supposed to convert a value to a memory
/// slice which `lmdb` uses to prevent multiple copying data
/// multiple times. May be unsafe.

pub trait ToMdbValue {
    fn to_mdb_value(&self) -> MdbValue<'_>;
}

/// `FromMdbValue` is supposed to reconstruct a value from
/// memory slice. It allows to use zero copy where it is
/// required.

pub trait FromMdbValue {
    fn from_mdb_value(value: &MdbValue) -> Self;
}

impl ToMdbValue for Vec<u8> {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        unsafe {
            MdbValue::new(std::mem::transmute::<*const u8, *const libc::c_void>(self.as_ptr()), self.len())
        }
    }
}

impl ToMdbValue for String {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        unsafe {
            let t: &'a str = self;
            MdbValue::new(std::mem::transmute::<*const u8, *const libc::c_void>(t.as_ptr()), t.len())
        }
    }
}

impl<'a> ToMdbValue for &'a str {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        unsafe {
            MdbValue::new(mem::transmute::<*const u8, *const libc::c_void>(self.as_ptr()), self.len())
        }
    }
}

impl<'a> ToMdbValue for &'a [u8] {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        unsafe {
            MdbValue::new(std::mem::transmute::<*const u8, *const libc::c_void>(self.as_ptr()),
                          self.len())
        }
    }
}

impl<'a> ToMdbValue for &'a u64 {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        unsafe {
            let l = self.to_be_bytes();
            MdbValue::new(std::mem::transmute::<[u8; 8], *const libc::c_void>(l), l.len())
        }
    }
}

impl<'a> ToMdbValue for &'a i64 {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        unsafe {
            let l = self.to_be_bytes();
            MdbValue::new(std::mem::transmute::<[u8; 8], *const libc::c_void>(l), l.len())
        }
    }
}


impl ToMdbValue for MDB_val {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        unsafe {
            MdbValue::from_raw(self)
        }
    }
}

impl<'a> ToMdbValue for MdbValue<'a> {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        *self
    }
}


impl FromMdbValue for String {
    fn from_mdb_value(value: &MdbValue) -> String {
        unsafe {
            let ptr = mem::transmute::<*const libc::c_void, *const u8>(value.get_ref());
            let data: Vec<u8> = slice::from_raw_parts(ptr, value.get_size()).to_vec();
            String::from_utf8(data).unwrap()
        }
    }
}

impl FromMdbValue for Vec<u8> {
    fn from_mdb_value(value: &MdbValue) -> Vec<u8> {
        unsafe {
            let ptr = mem::transmute::<*const libc::c_void, *const u8>(value.get_ref());
            slice::from_raw_parts(ptr, value.get_size()).to_vec()
        }
    }
}

impl FromMdbValue for () {
    fn from_mdb_value(_: &MdbValue) {
    }
}

impl<'b> FromMdbValue for &'b str {
    fn from_mdb_value(value: &MdbValue) -> &'b str {
        unsafe {
            std::mem::transmute(slice::from_raw_parts(value.get_ref(), value.get_size()))
        }
    }
}

impl<'b> FromMdbValue for &'b [u8] {
    fn from_mdb_value(value: &MdbValue) -> &'b [u8] {
        unsafe {
            std::mem::transmute(slice::from_raw_parts(value.get_ref(), value.get_size()))
        }
    }
}

