use std::{env, fs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;
use std::thread;

use libc::c_int;

use self::core::*;
use ffi::MDB_val;
use crate::traits::FromMdbValue;
use crate::*;

const USER_DIR: u32 = 0o777;
static TEST_ROOT_DIR: &'static str = "test-dbs";
static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
static INIT_DIR_ONCE: Once = Once::new();

fn global_root() -> PathBuf {
     let mut path = env::current_exe().unwrap();
     path.pop(); // chop off exe name
     path.pop(); // chop off 'debug'

     // If `cargo test` is run manually then our path looks like
     // `target/debug/foo`, in which case our `path` is already pointing at
     // `target`. If, however, `cargo test --target $target` is used then the
     // output is `target/$target/debug/foo`, so our path is pointing at
     // `target/$target`. Here we conditionally pop the `$target` name.
     if path.file_name().and_then(|s| s.to_str()) != Some("target") {
         path.pop();
     }

     path.join(TEST_ROOT_DIR)
 }

fn next_path() -> PathBuf {
    let root_dir = global_root();

    INIT_DIR_ONCE.call_once(|| {
        if let Ok(root_meta) = fs::metadata(root_dir.clone()) {
            if root_meta.is_dir() {
                let _ = fs::remove_dir_all(&root_dir);
            }
        }
        assert!(fs::create_dir_all(&root_dir).is_ok());
    });

    let cur_id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let res = root_dir.join(&format!("db-{}", cur_id));
    println!("Testing db in {}", res.display());
    res
}

#[test]
fn test_environment() {
    let mut env = EnvBuilder::new()
        .max_readers(33)
        .open(&next_path(), USER_DIR).unwrap();

    env.sync(true).unwrap();

    let test_flags = EnvFlags::EnvNoMemInit | EnvFlags::EnvNoMetaSync;

    env.set_flags(test_flags, true).unwrap();
    let new_flags = env.get_flags().unwrap();
    assert!((new_flags & test_flags) == test_flags, "Get flags != set flags");

    let db = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let key = "hello";
    let value = "world";

    db.set(&key, &value).unwrap();

    let v = db.get::<&str>(&key).unwrap();
    assert!(v == value, "Written {} and read {}", &value, &v);
}

#[test]
fn test_single_values() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist yet");

    assert!(db.set(&test_key1, &test_data1).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.set(&test_key1, &test_data2).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data2, "Data written differs from data read");

    assert!(db.del(&test_key1).is_ok());
    assert!(db.get::<()>(&test_key1).is_err(), "Key should be deleted");
}

#[test]
fn test_multiple_values() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::DbAllowDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist yet");

    assert!(db.set(&test_key1, &test_data1).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.set(&test_key1, &test_data2).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "It should still return first value");

    assert!(db.del_item(&test_key1, &test_data1).is_ok());

    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data2, "It should return second value");
    assert!(db.del(&test_key1).is_ok());

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist anymore!");
}

#[test]
fn test_append_duplicate() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::DbAllowDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.append(&test_key1, &test_data1).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.append_duplicate(&test_key1, &test_data2).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "It should still return first value");

    assert!(db.del_item(&test_key1, &test_data1).is_ok());

    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data2, "It should return second value");

    match db.append_duplicate(&test_key1, &test_data1).err().unwrap() {
        KeyExists => (),
        _ => panic!("Expected KeyExists error")
    }
}

#[test]
fn test_insert_values() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist yet");

    assert!(db.set(&test_key1, &test_data1).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.insert(&test_key1, &test_data2).is_err(), "Inserting should fail if key exists");

    assert!(db.del(&test_key1).is_ok());
    assert!(db.get::<()>(&test_key1).is_err(), "Key should be deleted");

    assert!(db.insert(&test_key1, &test_data2).is_ok(), "Inserting should succeed");
}

#[test]
fn test_resize_map() {
    use ffi::MDB_MAP_FULL;
    
    let env = EnvBuilder::new()
        .max_dbs(5)
        .map_size(0x1000u64)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::empty()).unwrap();

    let mut key_idx = 0u64;
    let test_data: [u8; 0xFF] = [0x5A; 0xFF];

    let mut write_closure = || {
        let txn = env.new_transaction().unwrap();
        {
            let db = txn.bind(&db);
            let test_key = format!("key_{}", key_idx);
            db.set(&test_key, &(&test_data[..]))?;
        }
        key_idx += 1;
        txn.commit()
    };
    // write data until running into 'MDB_MAP_FULL' error
    loop {
        match write_closure() {
            Err(MdbError::Other(MDB_MAP_FULL, _)) => { break; }
            Err(_) => panic!("unexpected db error"),
            _ => {} // continue
        }
    }

    // env should be still ok and resizable
    assert!(env.set_mapsize(0x100000usize).is_ok(), "Couldn't resize map");

    // next write after resize should not fail
    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        let test_key = "different_key";
        assert!(db.set(&test_key, &(&test_data[..])).is_ok(), "set after resize failed");
    }
    assert!(txn.commit().is_ok(), "Commit failed after resizing map");
}

#[test]
fn test_stat() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    // ~ the two dataset; each to end up in its own database
    let dss = [
        // ~ keep the "default db" dataset here at the beginning (see
        // the assertion at the end of this test)
        ("", vec![("default", "db"), ("has", "some"), ("extras", "prepared")]),
        ("db1", vec![("foo", "bar"), ("quux", "qak")]),
        ("db2", vec![("a", "abc"), ("b", "bcd"), ("c", "cde"), ("d", "def")]),
        ("db3", vec![("hip", "hop")])];

    // ~ create each db, populate it, and assert db.stat() for each seperately
    for &(name, ref ds) in &dss {
        let db = env.create_db(name, DbFlags::empty()).unwrap();
        let tx = env.new_transaction().unwrap();
        {
            let db = tx.bind(&db);
            for &(k, v) in ds {
                assert!(db.set(&k, &v).is_ok());
            }
            // ~ verify the expected number of entries (key/value pairs) in the db
            let stat = db.stat().unwrap();
            assert_eq!(ds.len() as usize, stat.ms_entries);
        }
        tx.commit().unwrap();
    }

    // ~ now verify the number of data items in this _environment_ (this
    // is the number key/value pairs in the default database plus the
    // number of other databases)
    let stat = env.stat().unwrap();
    assert_eq!(dss[0].1.len() + dss[1..].len(), stat.ms_entries);
}


#[test]
fn test_cursors() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::DbAllowDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_key2 = "key2";
    let test_values: Vec<&str> = vec!("value1", "value2", "value3", "value4");

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist yet");

    for t in test_values.iter() {
        let _ = db.set(&test_key1, t);
        let _ = db.set(&test_key2, t);
    }

    let mut cursor = db.new_cursor().unwrap();
    assert!(cursor.to_first().is_ok());

    assert!(cursor.to_key(&test_key1).is_ok());
    assert!(cursor.item_count().unwrap() == 4);

    assert!(cursor.del_item().is_ok());
    assert!(cursor.item_count().unwrap() == 3);

    assert!(cursor.to_key(&test_key1).is_ok());
    let new_value = "testme";

    assert!(cursor.replace(&new_value).is_ok());
    {
        let (_, v) = cursor.get::<(), &str>().unwrap();
        // NOTE: this asserting will work once new_value is
        // of the same length as it is inplace change
        assert!(v == new_value);
    }

    assert!(cursor.del_all().is_ok());
    assert!(cursor.to_key(&test_key1).is_err());

    assert!(cursor.to_key(&test_key2).is_ok());
}


#[test]
fn test_cursor_item_manip() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::DbAllowDups | DbFlags::DbAllowIntDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_val_3 = 3u64.to_be_bytes().to_vec();
    let test_val_4 = 4u64.to_be_bytes().to_vec();
    let test_val_5 = 5u64.to_be_bytes().to_vec();
    let test_val_6 = 6u64.to_be_bytes().to_vec();

    assert!(db.set(&test_key1, &test_val_3).is_ok());

    let mut cursor = db.new_cursor().unwrap();
    assert!(cursor.to_key(&test_key1).is_ok());

    let values: Vec<Vec<u8>> = db.item_iter(&test_key1).unwrap()
        .map(|cv| cv.get_value::<Vec<u8>>())
        .collect();
    assert_eq!(values, vec![test_val_3.to_vec()]);

    assert!(cursor.add_item(&test_val_4).is_ok());
    assert!(cursor.add_item(&test_val_5).is_ok());

    let values: Vec<Vec<u8>> = db.item_iter(&test_key1).unwrap()
        .map(|cv| cv.get_value::<Vec<u8>>())
        .collect();
    assert_eq!(values, vec![test_val_3.to_vec(), test_val_4.to_vec(), test_val_5.to_vec()]);

    assert!(cursor.replace(&test_val_6).is_ok());
    let values: Vec<Vec<u8>> = db.item_iter(&test_key1).unwrap()
        .map(|cv| cv.get_value::<Vec<u8>>())
        .collect();

    assert_eq!(values, vec![test_val_3, test_val_4, test_val_6]);
}

fn as_slices(v: &Vec<String>) -> Vec<&str> {
    v.iter().map(|s| &s[..]).collect::<Vec<&str>>()
}

#[test]
fn test_item_iter() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::DbAllowDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";
    let test_key2 = "key2";
    let test_key3 = "key3";

    assert!(db.set(&test_key1, &test_data1).is_ok());
    assert!(db.set(&test_key1, &test_data2).is_ok());
    assert!(db.set(&test_key2, &test_data1).is_ok());

    let iter = db.item_iter(&test_key1).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(as_slices(&values), vec![test_data1, test_data2]);

    let iter = db.item_iter(&test_key2).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(as_slices(&values), vec![test_data1]);

    let iter = db.item_iter(&test_key3).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(values.len(), 0);
}

#[test]
fn test_db_creation() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();
    assert!(env.create_db("test-db", DbFlags::empty()).is_ok());
}

#[test]
fn test_read_only_txn() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();
    env.get_reader().unwrap();
}

#[test]
fn test_cursor_in_txns() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    {
        let db = env.create_db("test1", DbFlags::DbAllowDups | DbFlags::DbAllowIntDups).unwrap();
        let txn = env.new_transaction().unwrap();
        {
            let db = txn.bind(&db);

            let cursor = db.new_cursor();
            assert!(cursor.is_ok());
        }
        assert!(txn.commit().is_ok());
    }

    {
        let db = env.create_db("test1", DbFlags::DbAllowDups | DbFlags::DbAllowIntDups).unwrap();
        let txn = env.new_transaction().unwrap();
        {
            let db = txn.bind(&db);

            let cursor = db.new_cursor();
            assert!(cursor.is_ok());
        }
        assert!(txn.commit().is_ok());
    }
}

#[test]
fn test_multithread_env() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let shared_env = env.clone();
    let key = "key";
    let value = "value";

    let _ = thread::spawn(move || {
        let db = shared_env.create_db("test1", DbFlags::empty()).unwrap();
        let txn = shared_env.new_transaction().unwrap();
        {
            let db = txn.bind(&db);
            assert!(db.set(&key, &value).is_ok());
        }
        assert!(txn.commit().is_ok());
    }).join();

    let db = env.create_db("test1", DbFlags::empty()).unwrap();
    let txn = env.get_reader().unwrap();
    let db = txn.bind(&db);
    let value2: String = db.get(&key).unwrap();
    assert_eq!(value, value2);
}

#[test]
fn test_keyrange_to() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbIntKey).unwrap();
    let one = 1_i32.to_be_bytes().to_vec();
    let two = 2_i32.to_be_bytes().to_vec();
    let three = 3_i32.to_be_bytes().to_vec();
    let four = 4_i32.to_be_bytes().to_vec();
    let five = 5_i32.to_be_bytes().to_vec();
    let six = 6_i32.to_be_bytes().to_vec();
    let keys:   Vec<Vec<u8>> = vec![one, two, three];
    let values: Vec<Vec<u8>> = vec![four, five, six];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in keys.clone().iter().zip(values.iter()) {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let last_idx = keys.len() - 1;
        let last_key: &Vec<u8> = &keys[last_idx];
        // last key is excluded
        let iter = db.keyrange_to(last_key).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<Vec<u8>>()).collect();
        assert_eq!(res, &values[..last_idx]);
    }
}

/// Test that selecting a key range with an upper bound smaller than
/// the smallest key in the db yields an empty range.
#[test]
fn test_keyrange_to_init_cursor() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbIntKey).unwrap();
    let ten = 10_u32.to_be_bytes().to_vec();
    let fifty = 50_u32.to_be_bytes().to_vec();
    let eleven = 11_u32.to_be_bytes().to_vec();
    let sixty = 60_u32.to_be_bytes().to_vec();
    let twelve = 12_u32.to_be_bytes().to_vec();
    let seventy = 70_u32.to_be_bytes().to_vec();
    let recs: Vec<(Vec<u8>, Vec<u8>)> = vec![(ten, fifty), (eleven, sixty), (twelve, seventy)];

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in recs.iter() {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        // last key is excluded
        let upper_bound: Vec<u8> = 1_u32.to_be_bytes().to_vec();
        let iter = db.keyrange_to(&upper_bound).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<Vec<u8>>()).collect();
        assert!(res.is_empty());
    }
}

#[test]
fn test_keyrange_from() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbIntKey).unwrap();
    let one = 1_u32.to_be_bytes().to_vec();
    let two = 2_u32.to_be_bytes().to_vec();
    let three = 3_u32.to_be_bytes().to_vec();
    let seven = 7_u32.to_be_bytes().to_vec();
    let five = 5_u32.to_be_bytes().to_vec();
    let six = 6_u32.to_be_bytes().to_vec();
    let keys:   Vec<Vec<u8>> = vec![one, two, three];
    let values: Vec<Vec<u8>> = vec![five, six, seven];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let start_idx = 1; // second key
        let last_key: &Vec<u8> = &keys[start_idx];
        let iter = db.keyrange_from(last_key).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<Vec<u8>>()).collect();
        assert_eq!(res, &values[start_idx..]);
    }
}

/// Test that selecting a key range with a lower bound greater than
/// the biggest key in the db yields an empty range.
#[test]
fn test_keyrange_from_init_cursor() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbIntKey).unwrap();
    let ten = 10_u32.to_be_bytes().to_vec();
    let fifty = 50_u32.to_be_bytes().to_vec();
    let eleven = 11_u32.to_be_bytes().to_vec();
    let sixty = 60_u32.to_be_bytes().to_vec();
    let twelve = 12_u32.to_be_bytes().to_vec();
    let seventy = 70_u32.to_be_bytes().to_vec();
    let recs: Vec<(Vec<u8>, Vec<u8>)> = vec![(ten, fifty), (eleven, sixty), (twelve, seventy)];

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in recs.clone().iter() {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        // last key is excluded
        let v_val = &recs.clone()[recs.clone().len()-1].0;
        let mut t_val: [u8; 4] = [0_u8; 4];
        for i in 0..t_val.len() {
            t_val[i] = v_val[i];
        }
        let lower_bound = (u32::from_be_bytes(t_val) + 1).to_be_bytes().to_vec();
        // let lower_bound = recs[recs.len()-1].0 + 1_u32.to_be_bytes().to_vec();
        let iter = db.keyrange_from(&lower_bound).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<Vec<u8>>()).collect();
        assert!(res.is_empty());
    }
}

#[test]
fn test_keyrange() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbAllowDups | DbFlags::DbIntKey).unwrap();
    let one = 1_u32.to_be_bytes().to_vec();
    let two = 2_u32.to_be_bytes().to_vec();
    let three = 3_u32.to_be_bytes().to_vec();
    let four = 4_u32.to_be_bytes().to_vec();
    let five = 5_u32.to_be_bytes().to_vec();
    let six = 6_u32.to_be_bytes().to_vec();
    let keys: Vec<Vec<u8>> = vec![one,  two,  three,  four,  five,  six];
    let ten = 10_u32.to_be_bytes().to_vec();
    let eleven = 11_u32.to_be_bytes().to_vec();
    let twelve = 12_u32.to_be_bytes().to_vec();
    let thirteen = 13_u32.to_be_bytes().to_vec();
    let fourteen = 14_u32.to_be_bytes().to_vec();
    let fifteen = 15_u32.to_be_bytes().to_vec();
    let values: Vec<Vec<u8>> = vec![ten, eleven, twelve, thirteen, fourteen, fifteen];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let start_idx = 1;
        let end_idx = 3;
        let iter = db.keyrange(&keys[start_idx], &keys[end_idx]).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<Vec<u8>>()).collect();

         //  +1 as Rust slices do not include end
        assert_eq!(res, &values[start_idx.. end_idx + 1]);
    }
}

/// Test that select a key range outside the available data correctly
/// yields an empty range.
#[test]
fn test_keyrange_init_cursor() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbAllowDups | DbFlags::DbIntKey).unwrap();
    let one = 1_u32.to_be_bytes().to_vec();
    let two = 2_u32.to_be_bytes().to_vec();
    let three = 3_u32.to_be_bytes().to_vec();
    let four = 4_u32.to_be_bytes().to_vec();
    let five = 5_u32.to_be_bytes().to_vec();
    let six = 6_u32.to_be_bytes().to_vec();
    let keys: Vec<Vec<u8>> = vec![one,  two,  three,  four,  five,  six];
    let ten = 10_u32.to_be_bytes().to_vec();
    let eleven = 11_u32.to_be_bytes().to_vec();
    let twelve = 12_u32.to_be_bytes().to_vec();
    let thirteen = 13_u32.to_be_bytes().to_vec();
    let fourteen = 14_u32.to_be_bytes().to_vec();
    let fifteen = 15_u32.to_be_bytes().to_vec();
    let values: Vec<Vec<u8>> = vec![ten, eleven, twelve, thirteen, fourteen, fifteen];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    // test the cursor initialization before the available data range
    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let zero = 0u64.to_be_bytes().to_vec();
        let start_key = zero.clone();
        let end_key = zero;
        let iter = db.keyrange(&start_key, &end_key).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<Vec<u8>>()).collect();
        assert!(res.is_empty());
    }

    // test the cursor initialization after the available data range
    {
        let db = txn.bind(&db);

        let start_key: Vec<u8> = 10_u32.to_be_bytes().to_vec();
        let end_key: Vec<u8> = 10_u32.to_be_bytes().to_vec();
        let iter = db.keyrange(&start_key, &end_key).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<Vec<u8>>()).collect();
        assert!(res.is_empty());
    }
}

#[test]
fn test_keyrange_from_to() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbAllowDups | DbFlags::DbIntKey).unwrap();
    let ten = 10_u32.to_be_bytes().to_vec();
    let eleven = 11_u32.to_be_bytes().to_vec();
    let twenty = 20_u32.to_be_bytes().to_vec();
    let twenty_one = 21_u32.to_be_bytes().to_vec();
    let thirty = 30_u32.to_be_bytes().to_vec();
    let thirty_one = 31_u32.to_be_bytes().to_vec();
    let fourty = 40_u32.to_be_bytes().to_vec();
    let fourty_one = 41_u32.to_be_bytes().to_vec();
    let fifty = 50_u32.to_be_bytes().to_vec();
    let fifty_one = 51_u32.to_be_bytes().to_vec();
    let recs: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (ten, eleven),
        (twenty, twenty_one),
        (thirty, thirty_one),
        (fourty, fourty_one),
        (fifty, fifty_one)
    ];

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in recs.clone().iter() {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let start_idx = 1;
        let end_idx = 3;
        let iter = db.keyrange_from_to(&recs[start_idx].0, &recs[end_idx].0).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<Vec<u8>>()).collect();
        // ~ end_key must be excluded here
        let exp: Vec<_> = recs[start_idx .. end_idx].iter().map(|x| x.1.clone()).collect();
        assert_eq!(res, exp);
    }
}

#[test]
fn test_readonly_env() {
    let ten = 10_u32.to_be_bytes().to_vec();
    let eleven = 11_u32.to_be_bytes().to_vec();
    let twelve = 12_u32.to_be_bytes().to_vec();
    let thirteen = 13_u32.to_be_bytes().to_vec();
    let fourteen = 14_u32.to_be_bytes().to_vec();
    let recs: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (ten, eleven.clone()), (eleven, twelve.clone()), (twelve, thirteen.clone()), (thirteen, fourteen)
    ];

    // ~ first create a new read-write environment with its default
    // database containing a few entries
    let path = next_path();
    {
        let rw_env = EnvBuilder::new().open(&path, USER_DIR).unwrap();
        let dbh = rw_env.get_default_db(DbFlags::DbIntKey).unwrap();
        let tx = rw_env.new_transaction().unwrap();
        {
            let db = tx.bind(&dbh);
            for rec in recs.clone().iter() {
                db.set(&rec.0, &rec.1).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    // ~ now re-open the previously created database in read-only mode
    // and iterate the key/value pairs
    let ro_env = EnvBuilder::new()
        .flags(EnvCreateFlags::EnvCreateReadOnly)
        .open(&path, USER_DIR).unwrap();
    let dbh = ro_env.get_default_db(DbFlags::DbIntKey).unwrap();
    assert!(ro_env.new_transaction().is_err());
    let mut tx = ro_env.get_reader().unwrap();
    {
        let db = tx.bind(&dbh);
        let kvs: Vec<(Vec<u8>, Vec<u8>)> = db.iter().unwrap().map(|c| c.get()).collect();
        assert_eq!(recs, kvs);
    }
    tx.abort();
}

unsafe fn negative_if_odd_i64_val(val: *const MDB_val) -> i32 {
    let v = MdbValue::from_raw(val);
    let v_val = Vec::from_mdb_value(&v);
    let mut t_val: [u8; 4] = [0_u8; 4];
    for i in 0..t_val.len() {
        t_val[i] = v_val[i];
    }
    let i = i32::from_be_bytes(t_val);
    if i % 2 == 0 {
        i
    } else {
        -i
    }
}

// A nonsensical comparison function that sorts differently that byte-by-byte comparison
extern "C" fn negative_odd_cmp_fn(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> c_int {
    unsafe {
        let lhs = negative_if_odd_i64_val(lhs_val);
        let rhs = negative_if_odd_i64_val(rhs_val);
        lhs - rhs
    }
}

#[test]
fn test_compare() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db_handle = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    let val: Vec<u8> = 0_u32.to_be_bytes().to_vec();
    let two = 2_u32.to_be_bytes().to_vec();
    let three = 3_u32.to_be_bytes().to_vec();
    let four = 4_u32.to_be_bytes().to_vec();
    let five = 5_u32.to_be_bytes().to_vec();
    {
        let db = txn.bind(&db_handle);
        assert!(db.set_compare(negative_odd_cmp_fn).is_ok());

        let i: Vec<u8> = two.clone();
        db.set(&i, &val).unwrap();
        let i: Vec<u8> = three.clone();
        db.set(&i, &val).unwrap();
    }
    assert!(txn.commit().is_ok());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db_handle);
        let i: Vec<u8> = four.clone();
        db.set(&i, &val).unwrap();
        let i: Vec<u8> = five.clone();
        db.set(&i, &val).unwrap();
    }
    assert!(txn.commit().is_ok());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db_handle);
        let keys: Vec<_> = db.iter().unwrap().map(|cv| cv.get_key::<Vec<u8>>()).collect();
        assert_eq!(keys, [five, three, two, four]);
    }
    assert!(txn.commit().is_ok());
}

#[test]
fn test_dupsort() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db_handle = env.get_default_db(DbFlags::DbAllowDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let key: Vec<u8> = 0_u32.to_be_bytes().to_vec();
    let two = 2_u32.to_be_bytes().to_vec();
    let three = 3_u32.to_be_bytes().to_vec();
    let four = 4_u32.to_be_bytes().to_vec();
    let five = 5_u32.to_be_bytes().to_vec();
    {
        let db = txn.bind(&db_handle);
        assert!(db.set_dupsort(negative_odd_cmp_fn).is_ok());

        let i: Vec<u8> = two.clone();
        db.set(&key, &i).unwrap();
        let i: Vec<u8> = three.clone();
        db.set(&key, &i).unwrap();
    }
    assert!(txn.commit().is_ok());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db_handle);
        let i: Vec<u8> = four.clone();
        db.set(&key, &i).unwrap();
        let i: Vec<u8> = five.clone();
        db.set(&key, &i).unwrap();
    }
    assert!(txn.commit().is_ok());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db_handle);
        let vals: Vec<_> = db.item_iter(&key).unwrap().map(|cv| cv.get_value::<Vec<u8>>()).collect();
        assert_eq!(vals, [five, three, two, four]);
    }
    assert!(txn.commit().is_ok());
}

// // ~ see #29
#[test]
fn test_conversion_to_vecu8() {
    let ten = 10_u32.to_be_bytes().to_vec();
    let rec: (Vec<u8>, Vec<u8>) = (ten, vec![1,2,3,4,5]);

    let path = next_path();
    let env = EnvBuilder::new().open(&path, USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbIntKey).unwrap();

    // ~ add our test record
    {
        let tx = env.new_transaction().unwrap();
        {
            let db = tx.bind(&db);
            db.set(&rec.0, &rec.1).unwrap();
        }
        tx.commit().unwrap();
    }

    // ~ validate the behavior
    let tx = env.new_transaction().unwrap();
    {
        let db = tx.bind(&db);
        {
            // ~ now retrieve a Vec<u8> and make sure it is dropped
            // earlier than our database handle
            let xs: Vec<u8> = db.get(&rec.0).unwrap();
            assert_eq!(rec.1, xs);
        }
    }
    tx.abort();
}

// // ~ see #29
#[test]
fn test_conversion_to_string() {
    let ten = 10_u32.to_be_bytes().to_vec();
    let rec: (Vec<u8>, String) = (ten, "hello, world".to_owned());

    let path = next_path();
    let env = EnvBuilder::new().open(&path, USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::DbIntKey).unwrap();

    // ~ add our test record
    {
        let tx = env.new_transaction().unwrap();
        {
            let db = tx.bind(&db);
            db.set(&rec.0, &rec.1).unwrap();
        }
        tx.commit().unwrap();
    }

    // ~ validate the behavior
    let tx = env.new_transaction().unwrap();
    {
        let db = tx.bind(&db);
        {
            // ~ now retrieve a String and make sure it is dropped
            // earlier than our database handle
            let xs: String = db.get(&rec.0).unwrap();
            assert_eq!(rec.1, xs);
        }
    }
    tx.abort();
}
