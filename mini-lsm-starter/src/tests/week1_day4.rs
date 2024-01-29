use std::fs::{File, OpenOptions};
use std::io::{BufRead, Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::sync::Arc;
use anyhow::Result;

use bytes::Bytes;
use tempfile::{tempdir, TempDir};
use std::path::{Path, PathBuf};
use crate::iterators::StorageIterator;
use crate::table::{self, SsTable, SsTableBuilder, SsTableIterator};

#[test]
fn test_sst_build_single_key() {
    let mut builder = SsTableBuilder::new(16);
    builder.add(b"233", b"233333");
    
    let dir = tempdir().unwrap();
    // let dir = PathBuf::from("./src/tests/day4_test").join("1.sst");
    builder.build_for_test(dir.path().join("1.sst")).unwrap();
}

#[test]
fn file_io_test1() {
    use std::io::BufReader;
    let mut file = File::create("test.txt").expect("error");
    file.write_all(b"hello").expect("error");

    let file = File::open("test.txt").expect("error");
    let reader = BufReader::new(file);
    for line in reader.lines() {
        println!("{}", line.expect("Unable to read line"));
    }
}

#[test]
fn file_io_test2() ->Result<()> {
    File::create("src/tests/day4_test/1.sst")?;
    
    let pathbuf = PathBuf::from("src/tests/day4_test").join("2.sst.txt");
    let contents = "hello world".to_string();
    std::fs::write(pathbuf.as_path(), contents)?;
    let file = OpenOptions::new().write(true).open(pathbuf.as_path()).unwrap();
    file.sync_all()?;
    Ok(())
}

#[test]
fn test_sst_build_two_blocks() {
    let mut builder = SsTableBuilder::new(16);
    builder.add(b"11", b"11");
    builder.add(b"22", b"22");
    builder.add(b"33", b"11");
    builder.add(b"44", b"22");
    builder.add(b"55", b"11");
    builder.add(b"66", b"22");
    let sst = builder.build_for_test("./test").unwrap();
    assert!(sst.block_meta.len() >= 2);
}

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    100
}

fn generate_sst() -> (TempDir, SsTable) {
    let mut builder = SsTableBuilder::new(128);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(&key[..], &value[..]);
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    (dir, builder.build_for_test(path).unwrap())
}

#[test]
fn test_sst_build_all() {
    generate_sst();
}

#[test]
fn test_sst_decode() {
    let (_dir, sst) = generate_sst();
    let meta = sst.block_meta.clone();
    let new_sst = SsTable::open_for_test(sst.file).unwrap();
    assert_eq!(new_sst.block_meta, meta);
    assert_eq!(new_sst.first_key(), &key_of(0));
    assert_eq!(new_sst.last_key(), &key_of(num_of_keys() - 1));
}


fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[test]
fn test_sst_iterator() {
    let (_dir, sst) = generate_sst();
    let sst = Arc::new(sst);
    let mut iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
    for _ in 0..5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key,
                key_of(i),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(&key_of(i)),
                as_bytes(key)
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.next().unwrap();
        }
        iter.seek_to_first().unwrap();
    }
}

#[test]
fn test_sst_seek_key() {
    let (_dir, sst) = generate_sst();
    let sst = Arc::new(sst);
    let mut iter = SsTableIterator::create_and_seek_to_key(sst, &key_of(0)).unwrap();
    for offset in 1..=5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key,
                key_of(i),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(&key_of(i)),
                as_bytes(key)
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.seek_to_key(&format!("key_{:03}", i * 5 + offset).into_bytes())
                .unwrap();
        }
        iter.seek_to_key(b"k").unwrap();
    }
}
