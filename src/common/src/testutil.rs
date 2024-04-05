use crate::prelude::*;
use crate::table::TableInfo;
use crate::{Attribute, DataType, Field, TableSchema, Tuple};
use itertools::izip;
use rand::distributions::Alphanumeric;
use rand::{
    distributions::{Distribution, Uniform},
    thread_rng, Rng,
};
use std::env;
use std::path::{Path, PathBuf};

pub fn init() {
    // To change the log level for tests change the filter_level
    let _ = env_logger::builder()
        .is_test(true)
        //.filter_level(log::LevelFilter::Trace)
        .filter_level(log::LevelFilter::Info)
        .try_init();
}

pub fn gen_uniform_strings(n: u64, cardinality: Option<u64>, min: usize, max: usize) -> Vec<Field> {
    let mut rng = rand::thread_rng();
    let mut ret: Vec<Field> = Vec::new();
    if let Some(card) = cardinality {
        let values: Vec<Field> = (0..card)
            .map(|_| Field::String(gen_rand_string_range(min, max)))
            .collect();
        assert_eq!(card as usize, values.len());
        //ret = values.iter().choose_multiple(&mut rng, n as usize).collect();
        let uniform = Uniform::new(0, values.len());
        for _ in 0..n {
            let idx = uniform.sample(&mut rng);
            assert!(idx < card as usize);
            ret.push(values[idx].clone())
        }
        //ret = rng.sample(values, n);
    } else {
        for _ in 0..n {
            ret.push(Field::String(gen_rand_string_range(min, max)))
        }
    }
    ret
}

pub fn gen_uniform_ints(n: u64, cardinality: Option<u64>) -> Vec<Field> {
    let mut rng = rand::thread_rng();
    let mut ret = Vec::new();
    if let Some(card) = cardinality {
        if card > i32::MAX as u64 {
            panic!("Cardinality larger than i32 max")
        }
        if n == card {
            // all values distinct
            if n < i32::MAX as u64 / 2 {
                for i in 0..card as i64 {
                    ret.push(Field::Int(i));
                }
            } else {
                for i in i64::MIN..i64::MIN + (card as i64) {
                    ret.push(Field::Int(i));
                }
            }
            //ret.shuffle(&mut rng);
        } else {
            let mut range = Uniform::new_inclusive(i64::MIN, i64::MIN + (card as i64) - 1);
            if card < (i32::MAX / 2) as u64 {
                range = Uniform::new_inclusive(0, card as i64 - 1);
            }
            for _ in 0..n {
                ret.push(Field::Int(range.sample(&mut rng)));
            }
        }
    } else {
        for _ in 0..n {
            ret.push(Field::Int(rng.gen::<i64>()));
        }
    }
    ret
}

pub fn gen_test_tuples(n: u64) -> Vec<Tuple> {
    let keys = gen_uniform_ints(n, Some(n));
    let i1 = gen_uniform_ints(n, Some(10));
    let i2 = gen_uniform_ints(n, Some(100));
    let i3 = gen_uniform_ints(n, Some(1000));
    let i4 = gen_uniform_ints(n, Some(10000));
    let s1 = gen_uniform_strings(n, Some(10), 10, 20);
    let s2 = gen_uniform_strings(n, Some(100), 10, 20);
    let s3 = gen_uniform_strings(n, Some(1000), 10, 20);
    let s4 = gen_uniform_strings(n, Some(10000), 10, 30);
    let mut tuples = Vec::new();
    for (k, a, b, c, d, e, f, g, h) in izip!(keys, i1, i2, i3, i4, s1, s2, s3, s4) {
        let vals: Vec<Field> = vec![k, a, b, c, d, e, f, g, h];
        tuples.push(Tuple::new(vals));
    }
    tuples
}

pub fn gen_test_table_and_tuples(c_id: ContainerId, n: u64) -> (TableInfo, Vec<Tuple>) {
    let table_name = format!("test_table_{}", c_id);
    let table = gen_table_for_test_tuples(c_id, table_name);
    let tuples = gen_test_tuples(n);
    (table, tuples)
}

/// Generates a table with the given name and schema to match the
/// the gen_test_tuples.
pub fn gen_table_for_test_tuples(c_id: ContainerId, table_name: String) -> TableInfo {
    let mut attributes: Vec<Attribute> = Vec::new();
    let pk_attr = Attribute {
        name: String::from("id"),
        dtype: DataType::Int,
        constraint: crate::Constraint::PrimaryKey,
    };
    attributes.push(pk_attr);

    for n in 1..5 {
        let attr = Attribute {
            name: format!("ia{}", n),
            dtype: DataType::Int,
            constraint: crate::Constraint::None,
        };
        attributes.push(attr);
    }
    for n in 1..5 {
        let attr = Attribute {
            name: format!("sa{}", n),
            dtype: DataType::String,
            constraint: crate::Constraint::None,
        };
        attributes.push(attr);
    }
    let table_schema = TableSchema::new(attributes);

    TableInfo::new(c_id, table_name, table_schema)
}

/// Converts an int vector to a Tuple.
///
/// # Argument
///
/// * `data` - Data to put into tuple.
pub fn int_vec_to_tuple(data: Vec<i64>) -> Tuple {
    let mut tuple_data = Vec::new();

    for val in data {
        tuple_data.push(Field::Int(val));
    }

    Tuple::new(tuple_data)
}

/// Creates a Vec of tuples containing Ints given a 2D Vec of i32 's
pub fn create_tuple_list(tuple_data: Vec<Vec<i64>>) -> Vec<Tuple> {
    let mut tuples = Vec::new();
    for item in &tuple_data {
        let fields = item.iter().map(|i| Field::Int(*i)).collect();
        tuples.push(Tuple::new(fields));
    }
    tuples
}

/// Creates a new table schema for a table with width number of Ints.
pub fn get_int_table_schema(width: usize) -> TableSchema {
    let mut attrs = Vec::new();
    for _ in 0..width {
        attrs.push(Attribute::new(String::new(), DataType::Int))
    }
    TableSchema::new(attrs)
}

pub fn get_random_byte_vec(n: usize) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..n).map(|_| rand::random::<u8>()).collect();
    random_bytes
}

pub fn gen_rand_string_range(min: usize, max: usize) -> String {
    if min >= max {
        return gen_rand_string(min);
    }
    let mut rng = rand::thread_rng();
    let size = rng.gen_range(min..max);
    thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}

pub fn gen_rand_string(n: usize) -> String {
    thread_rng()
        .sample_iter(Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

pub fn gen_random_test_sm_dir() -> PathBuf {
    init();
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut check_file = dir.clone();
    check_file.set_file_name(String::from(".CRUSTYROOT"));
    let mut found_root = Path::new(&check_file).exists();
    while !found_root {
        dir.push(String::from(".."));
        check_file = dir.clone();
        check_file.set_file_name(String::from(".CRUSTYROOT"));
        found_root = Path::new(&check_file).exists();
    }
    dir.push(String::from("crusty_data"));
    dir.push(String::from("temp"));
    let rand_string = gen_rand_string(10);
    dir.push(rand_string);
    dir
}

pub fn get_random_vec_of_byte_vec(n: usize, min_size: usize, max_size: usize) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    let mut rng = rand::thread_rng();
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.gen_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| rand::random::<u8>()).collect());
    }
    res
}

//get_ascending_vec_of_byte_vec_0x: this function will create Vec<Vec<u8>>
//the value of u8 in Vec<u8> is ascending from 0 to 16 (0x10) for each Vec<u8>
pub fn get_ascending_vec_of_byte_vec_0x(
    n: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    let mut rng = rand::thread_rng();
    let mut elements = 1;
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.gen_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| elements).collect());
        elements += 1;
        if elements >= 16 {
            elements = 1;
        }
    }
    res
}

//get_ascending_vec_of_byte_vec_0x: this function will create Vec<Vec<u8>>
//the value of u8 in Vec<u8> is ascending from 0 to 255 (0x100) for each Vec<u8>
pub fn get_ascending_vec_of_byte_vec_02x(
    n: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    let mut rng = rand::thread_rng();
    let mut elements = 1;
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.gen_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| elements).collect());
        if elements == 255 {
            elements = 1;
        } else {
            elements += 1;
        }
    }
    res
}

pub fn compare_unordered_byte_vecs(a: &[Vec<u8>], mut b: Vec<Vec<u8>>) -> bool {
    // Quick check
    if a.len() != b.len() {
        trace!("Vecs are different lengths");
        return false;
    }
    // check if they are the same ordered
    let non_match_count = a
        .iter()
        .zip(b.iter())
        .filter(|&(j, k)| j[..] != k[..])
        .count();
    if non_match_count == 0 {
        return true;
    }

    // Now check if they are out of order
    for x in a {
        let pos = b.iter().position(|y| y[..] == x[..]);
        match pos {
            None => {
                //Was not found, not equal
                trace!("Was not able to find value for {:?}", x);
                return false;
            }
            Some(idx) => {
                b.swap_remove(idx);
            }
        }
    }
    if !b.is_empty() {
        trace!("Values in B that did not match a {:?}", b);
    }
    //since they are the same size, b should be empty
    b.is_empty()
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_random_vec_bytes() {
        let n = 10_000;
        let mut min = 50;
        let mut max = 75;
        let mut data = get_random_vec_of_byte_vec(n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }

        min = 134;
        max = 134;
        data = get_random_vec_of_byte_vec(n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(x.len() == min && x.len() == max);
        }

        min = 0;
        max = 14;
        data = get_random_vec_of_byte_vec(n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }
    }

    #[test]
    fn test_ascd_random_vec_bytes() {
        let n = 10000;
        let mut min = 50;
        let mut max = 75;
        let mut data = get_ascending_vec_of_byte_vec_02x(n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            if x.len() < min || x.len() >= max {
                println!("!!!{:?}", x);
            }
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }

        min = 13;
        max = 14;
        data = get_ascending_vec_of_byte_vec_02x(n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            if x.len() != min || x.len() != max {
                println!("!!!{:?}", x);
                println!("!!!x.len(){:?}", x.len());
                println!("111{}", x.len() == min && x.len() == max);
            }
            assert!(x.len() == min && x.len() == max - 1);
        }

        min = 0;
        max = 14;
        data = get_ascending_vec_of_byte_vec_02x(n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }
    }

    #[test]
    fn test_tuple_gen() {
        let t = gen_test_tuples(10);
        assert_eq!(10, t.len());
    }

    #[test]
    fn test_uniform_strings() {
        let mut card = 10;
        let mut strs = gen_uniform_strings(100, Some(card), 10, 20);
        let mut map = HashMap::new();

        for x in &strs {
            if let Field::String(val) = x {
                assert!(val.len() < 20);
            }
        }
        assert_eq!(100, strs.len());
        for i in strs {
            if let Field::String(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card as usize, map.keys().len());

        card = 100;
        map.clear();
        strs = gen_uniform_strings(4800, Some(card), 10, 20);
        for i in strs {
            if let Field::String(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card as usize, map.keys().len());
    }

    #[test]
    fn test_uniform_range() {
        let n = 1000;
        let ints = gen_uniform_ints(n, Some(10));
        for x in &ints {
            if let Field::Int(a) = x {
                assert!(*a < 10 && *a >= 0);
            }
        }
    }

    #[test]
    fn test_uniform_ints() {
        let mut ints = gen_uniform_ints(4, Some(6));
        for x in &ints {
            if let Field::Int(a) = x {
                assert!(*a < 7);
            }
        }
        let mut card: usize = 20;
        ints = gen_uniform_ints(1000, Some(card as u64));
        assert_eq!(1000, ints.len());

        let mut map = HashMap::new();
        for i in ints {
            if let Field::Int(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card, map.keys().cloned().count());

        card = 121;
        map.clear();
        ints = gen_uniform_ints(10000, Some(card as u64));
        assert_eq!(10000, ints.len());
        let mut map = HashMap::new();
        for i in ints {
            if let Field::Int(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card, map.keys().cloned().count());

        card = 500;
        map.clear();
        ints = gen_uniform_ints(card as u64, Some(card as u64));
        let mut map = HashMap::new();
        for i in ints {
            if let Field::Int(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card, map.keys().cloned().count());
    }

    /*use rand::seq::SliceRandom;
    use rand::thread_rng;*/
    /*#[test]
    fn test_compare() {
        let mut rng = thread_rng();
        let a = get_random_vec_of_byte_vec(100, 10, 20);
        let b = a.clone();
        assert!(true, compare_unordered_byte_vecs(&a, b));
        let mut b = a.clone();
        b.shuffle(&mut rng);
        assert!(true, compare_unordered_byte_vecs(&a, b));
        let new_rand = get_random_vec_of_byte_vec(99, 10, 20);
        assert!(false, compare_unordered_byte_vecs(&a, new_rand));
        let mut b = a.clone();
        b[rng.gen_range(0..a.len())] = get_random_byte_vec(10);
        assert!(false, compare_unordered_byte_vecs(&a, b));
    }*/
}
