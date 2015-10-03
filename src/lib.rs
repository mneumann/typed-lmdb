extern crate lmdb_rs as lmdb;

use lmdb::{FromMdbValue, ToMdbValue, MdbValue,
           DbFlags, DbHandle, Database, Environment, Cursor, MDB_val};
use lmdb::core::MdbResult;
use std::marker::PhantomData;

#[macro_export]
macro_rules! lmdb_not_found {
    (
        $x:expr
    ) => {
        match $x {
            Err(lmdb::MdbError::NotFound) => true,
            _ => false
        }
    };
}

#[macro_export]
macro_rules! impl_table {
    (
        $s:ident, $k:ty, $v:ty
    ) => {
        impl $s {
            pub fn table<'db>(db: ::lmdb::Database<'db>) -> ::lmdb::core::MdbResult<Table<'db, $k, $v>> {
                try!(Self::setup(&db));
                Ok(Table::new(db))
            }
        }
    };
}

/// Defines all neccessary information to create/open a database.
pub trait TableDef {
    fn name() -> &'static str;
    fn flags() -> DbFlags;
    fn setup(db: &Database) -> MdbResult<()>;

    fn open(env: &Environment, create: bool) -> MdbResult<DbHandle> {
        if create {
            env.create_db(Self::name(), Self::flags())
        } else {
            env.get_db(Self::name(), Self::flags())
        }
    }
}

/// Wraps a lmdb::Database, keeping type information for K and V.
pub struct Table<'db, K, V> {
    db: Database<'db>,
    k: PhantomData<K>,
    v: PhantomData<V>,
}

impl<'db, K, V> Table<'db, K, V>
where K: FromMdbValue + ToMdbValue,
      V: FromMdbValue + ToMdbValue,
{
    #[inline(always)]
    pub fn new(db: Database<'db>) -> Table<'db, K, V> {
        Table {db: db, k: PhantomData, v: PhantomData}
    }

    #[inline(always)]
    pub fn set(&self, key: &K, value: &V) -> MdbResult<()> {
        self.db.set(key, value)
    }

    #[inline(always)]
    pub fn insert(&self, key: &K, value: &V) -> MdbResult<()> {
        self.db.insert(key, value)
    }

    /// Checks if the item exists.
    pub fn insert_item(&self, key: &K, value: &V) -> MdbResult<()> {
        if try!(self.has_item(key, value)) {
            return Err(::lmdb::MdbError::KeyExists);
        }
        self.set(key, value)
    }

    /// Returns true if item exists.
    pub fn has_item(&self, key: &K, value: &V) -> MdbResult<bool> {
        let mut cursor = try!(self.new_cursor());
        match cursor.to_item(key, value) {
            Ok(()) => Ok(true),
            Err(::lmdb::MdbError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> MdbResult<V> {
        self.db.get(key)
    }

    /// Returns ```alt``` if key was not found.
    #[inline(always)]
    pub fn get_or(&self, key: &K, alt: V) -> MdbResult<V> {
        match self.db.get(key) {
            Ok(v) => Ok(v),
            Err(::lmdb::MdbError::NotFound) => Ok(alt),
            Err(e) => Err(e),
        }
    }

    #[inline(always)]
    pub fn get_ref<'a, V2>(&self, key: &K) -> MdbResult<&'a V2> where &'a V2: FromMdbValue {
        self.db.get(key)
    }

    #[inline(always)]
    pub fn del(&self, key: &K) -> MdbResult<()> {
        self.db.del(key)
    }

    #[inline(always)]
    pub fn del_item(&self, key: &K, value: &V) -> MdbResult<()> {
        self.db.del_item(key, value)
    }

    #[inline(always)]
    pub fn new_cursor<'table>(&'table self) -> MdbResult<TypedCursor<'table, K, V>> {
        Ok(TypedCursor {cursor: try!(self.db.new_cursor()), k: PhantomData, v: PhantomData})
    }

    #[inline(always)]
    pub fn cursor_at_key<'table>(&'table self, k: &K) -> MdbResult<TypedCursor<'table, K, V>> {
        let mut cursor = TypedCursor {cursor: try!(self.db.new_cursor()), k: PhantomData, v: PhantomData};
        try!(cursor.to_key(k));
        Ok(cursor)
    }
}

/// Is a typed version of lmdb::Cursor.
pub struct TypedCursor<'table, K, V> {
    cursor: Cursor<'table>,
    k: PhantomData<K>,
    v: PhantomData<V>,
}

impl<'table, K: FromMdbValue+ToMdbValue, V: FromMdbValue+ToMdbValue> TypedCursor<'table, K, V> {
    #[inline(always)]
    pub fn to_first(&mut self) -> MdbResult<()> {
        self.cursor.to_first()
    }

    #[inline(always)]
    pub fn to_last(&mut self) -> MdbResult<()> {
        self.cursor.to_last()
    }

    #[inline(always)]
    pub fn to_key(&mut self, key: &K) -> MdbResult<()> {
        self.cursor.to_key(key)
    }

    #[inline(always)]
    pub fn to_gte_key(&mut self, key: &K) -> MdbResult<()> {
        self.cursor.to_gte_key(key)
    }

    #[inline(always)]
    pub fn to_next_key(&mut self) -> MdbResult<()> {
        self.cursor.to_next_key()
    }

    #[inline(always)]
    pub fn to_prev_key(&mut self) -> MdbResult<()> {
        self.cursor.to_prev_key()
    }

    #[inline(always)]
    pub fn to_item(&mut self, key: &K, value: &V) -> MdbResult<()> {
        self.cursor.to_item(key, value)
    }

    #[inline(always)]
    pub fn to_gte_item(&mut self, key: &K, value: &V) -> MdbResult<()> {
        self.cursor.to_gte_item(key, value)
    }

    #[inline(always)]
    pub fn to_first_item(&mut self) -> MdbResult<()> {
        self.cursor.to_first_item()
    }

    #[inline(always)]
    pub fn to_last_item(&mut self) -> MdbResult<()> {
        self.cursor.to_last_item()
    }

    #[inline(always)]
    pub fn to_next_item(&mut self) -> MdbResult<()> {
        self.cursor.to_next_item()
    }

    #[inline(always)]
    pub fn to_prev_item(&mut self) -> MdbResult<()> {
        self.cursor.to_prev_item()
    }

    #[inline(always)]
    pub fn item_count(&mut self) -> MdbResult<u64> {
        self.cursor.item_count()
    }

    #[inline(always)]
    pub fn get(&mut self) -> MdbResult<(K, V)> {
        self.cursor.get()
    }

    #[inline(always)]
    pub fn get_value(&mut self) -> MdbResult<V> {
        self.cursor.get_value()
    }

    #[inline(always)]
    pub fn get_value_ref<'a, V2>(&mut self) -> MdbResult<&'a V2> where &'a V2: FromMdbValue {
        self.cursor.get_value()
    }

    #[inline(always)]
    pub fn get_key(&mut self) -> MdbResult<K> {
        self.cursor.get_key()
    }

    #[inline(always)]
    pub fn replace(&mut self, value: &V) -> MdbResult<()> {
        self.cursor.replace(value)
    }

    #[inline(always)]
    pub fn add_item(&mut self, value: &V) -> MdbResult<()> {
        self.cursor.add_item(value)
    }

    #[inline(always)]
    pub fn del(&mut self) -> MdbResult<()> {
        self.cursor.del()
    }

    #[inline(always)]
    pub fn del_item(&mut self) -> MdbResult<()> {
        self.cursor.del_item()
    }

    #[inline(always)]
    pub fn del_all(&mut self) -> MdbResult<()> {
        self.cursor.del_all()
    }
}

pub extern "C" fn sort<T:FromMdbValue+Ord>(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> lmdb::c_int {
    let lhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(lhs_val)});
    let rhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(rhs_val)});

    let order: i8 = unsafe { std::mem::transmute(lhs.cmp(&rhs)) };
    order as lmdb::c_int
}

pub extern "C" fn sort_reverse<T:FromMdbValue+Ord>(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> lmdb::c_int {
    let lhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(lhs_val)});
    let rhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(rhs_val)});
    let order: i8 = unsafe { std::mem::transmute(lhs.cmp(&rhs).reverse()) };
    order as lmdb::c_int
}

#[test]
fn test_simple_table() {
    use std::path::Path;
    use lmdb::core::{DbIntKey, DbAllowDups, DbAllowIntDups, DbDupFixed};

    let env = lmdb::EnvBuilder::new().max_dbs(2).autocreate_dir(true).open(&Path::new("./test/db1"), 0o777).unwrap();

    struct MyFirstTable;
    impl TableDef for MyFirstTable {
        fn name() -> &'static str { "my_first_table" }
        fn flags() -> DbFlags { DbIntKey | DbAllowDups | DbAllowIntDups | DbDupFixed }
        fn setup(db: &Database) -> MdbResult<()> {
            db.set_dupsort(sort_reverse::<u64>)
        }
    }
    impl MyFirstTable {
        fn table<'db>(db: Database<'db>) -> MdbResult<Table<'db, u64, u64>> {
            try!(Self::setup(&db));
            Ok(Table{db: db, k: PhantomData, v: PhantomData})
        }
    }

    struct MyBlobTable;
    impl TableDef for MyBlobTable {
        fn name() -> &'static str { "my_blob_table" }
        fn flags() -> DbFlags { DbIntKey }
        fn setup(_db: &Database) -> MdbResult<()> { Ok(()) }
    }
    impl MyBlobTable {
        fn table<'db>(db: Database<'db>) -> MdbResult<Table<'db, u64, &[u8]>> {
            try!(Self::setup(&db));
            Ok(Table{db: db, k: PhantomData, v: PhantomData})
        }
    }

    // A Unique(Id64, Id64 DESC) table
    let table_handle = MyFirstTable::open(&env, true).unwrap();
    let blobs_handle = MyBlobTable::open(&env, true).unwrap();

    // prepare database
    {
        let txn = env.new_transaction().unwrap();
        {
            let table = MyFirstTable::table(txn.bind(&table_handle)).unwrap(); 
            let blobs = MyBlobTable::table(txn.bind(&blobs_handle)).unwrap();

            let big_blob: &[u8] = b"Test";
            blobs.set(&1, &big_blob).unwrap();

            let back: &[u8] = blobs.get(&1).unwrap();
            assert_eq!(&back[..], &b"Test"[..]);

            let key = 1;
            table.set(&key, &200).unwrap();
            table.set(&key, &200).unwrap();
            table.set(&key, &100).unwrap();
            table.set(&key, &2).unwrap();
            table.set(&key, &2).unwrap();
            table.set(&key, &2).unwrap();
            table.set(&key, &2).unwrap();
            table.set(&key, &1).unwrap();
            table.set(&key, &2).unwrap();
            table.set(&key, &3).unwrap();
            table.set(&key, &100).unwrap();
        }
        txn.commit().unwrap();
    }
    // read
    {
        let rdr = env.get_reader().unwrap();

        let table = MyFirstTable::table(rdr.bind(&table_handle)).unwrap(); 

        let mut cursor = table.new_cursor().unwrap();
        cursor.to_key(&1).unwrap(); //  positions on first item of key
        assert_eq!((1, 200), cursor.get().unwrap());

        cursor.to_next_item().unwrap();
        assert_eq!((1, 100), cursor.get().unwrap());

        cursor.to_next_item().unwrap();
        assert_eq!((1, 3), cursor.get().unwrap());

        cursor.to_next_item().unwrap();
        assert_eq!((1, 2), cursor.get().unwrap());

        cursor.to_next_item().unwrap();
        assert_eq!((1, 1), cursor.get().unwrap());

        assert!(lmdb_not_found!(cursor.to_next_item()));

        let mut cursor = table.new_cursor().unwrap();
        {
            assert!(lmdb_not_found!(cursor.to_item(&2, &3)));

            cursor.to_gte_item(&1, &4).unwrap();
            assert_eq!((1, 3), cursor.get().unwrap());

            cursor.to_next_item().unwrap();
            assert_eq!((1, 2), cursor.get().unwrap());
       
            cursor.to_next_item().unwrap();
            assert_eq!((1, 1), cursor.get().unwrap());
 
            assert!(lmdb_not_found!(cursor.to_next_item()));
        }
    }
}
