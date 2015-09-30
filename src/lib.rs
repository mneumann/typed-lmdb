extern crate lmdb_rs as lmdb;

use lmdb::{FromMdbValue, ToMdbValue, MdbValue, DbFlags, DbHandle, Database, Environment, Transaction, ReadonlyTransaction, Cursor, MDB_val};
use lmdb::core::MdbResult;
use std::marker::PhantomData;

#[macro_export]
macro_rules! table_def {
    (
        pub $structname:ident, $tableclass:ty, $str:expr
    ) => {
        #[derive(Clone)]
        pub struct $structname;
        impl TableDef for $structname {
            type C = $tableclass;
            fn table_name() -> &'static str { $str }
        }
    };
    (
        $structname:ident, $tableclass:ty, $str:expr
    ) => {
        #[derive(Clone)]
        struct $structname;
        impl TableDef for $structname {
            type C = $tableclass; 
            fn table_name() -> &'static str { $str }
        }
    };

}

#[macro_export]
macro_rules! table_def_lifetime {
    (
        $structname:ident, $tableclass:ty, $str:expr
    ) => {
        struct $structname<'a> {marker: PhantomData<&'a()>}
        impl<'a> TableDef for $structname<'a> {
            type C = $tableclass; 
            fn table_name() -> &'static str { $str }
        }
    };
}

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

/// A TableClass describes the types of key/value and the behaviour (sort function) of a Table.
pub trait TableClass: Sized {
    type Key: ToMdbValue + FromMdbValue;
    type Value: ToMdbValue + FromMdbValue;

    /// Flags used for creation/open
    fn dbflags() -> DbFlags;

    /// Is called before accessing a database. Used to setup custom sort functions etc. 
    fn prepare_database(db: &Database) -> MdbResult<()>;
}

/// A TableDef is a concrete instance of a TableClass with a given table name.
pub trait TableDef: Sized {
    type C: TableClass;
    fn table_name() -> &'static str;

    fn create_table(env: &Environment) -> MdbResult<TableHandle<Self>> {
        env.create_db(Self::table_name(), Self::C::dbflags()).map(|dbh| {
            TableHandle {
                dbh: dbh,
                tabledef: PhantomData,
            }
        })
    }

    fn open_table(env: &Environment) -> MdbResult<TableHandle<Self>> {
        env.get_db(Self::table_name(), Self::C::dbflags()).map(|dbh| {
            TableHandle {
                dbh: dbh,
                tabledef: PhantomData,
            }
        })
    }

}

/// Wraps a lmdb::DbHandle, keeping the TableDef in the type signature.
#[derive(Clone)]
pub struct TableHandle<D: TableDef> {
    dbh: DbHandle,
    tabledef: PhantomData<D>,
}

impl<D: TableDef> TableHandle<D> {
    /// Bind a TableHandle towards a transaction. Returns a Table which you can use to
    /// modify the database.
    #[inline(always)]
    pub fn bind_transaction<'txn>(&self, txn: &'txn Transaction) -> MdbResult<Table<'txn, D>> {
        let db = txn.bind(&self.dbh);
        try!(D::C::prepare_database(&db));
        Ok(Table {
            db: db,
            tabledef: PhantomData,
        })
    }

    /// Bind a TableHandle towards a readonly transaction.
    /// XXX: This should return a ReadonlyTable?
    #[inline(always)]
    pub fn bind_reader<'txn>(&self, txn: &'txn ReadonlyTransaction) -> MdbResult<Table<'txn, D>> {
        let db = txn.bind(&self.dbh);
        try!(D::C::prepare_database(&db));
        Ok(Table {
                db: db,
                tabledef: PhantomData,
            } 
        )
    }

}

/// Wraps a lmdb::Database, keeping the TableDef in the type signature.
pub struct Table<'db, D: TableDef> {
    db: Database<'db>,
    tabledef: PhantomData<D>,
}

impl<'db, D: TableDef> Table<'db, D> {
    #[inline(always)]
    pub fn set(&self, key: &<<D as TableDef>::C as TableClass>::Key, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
        self.db.set(key, value)
    }

    #[inline(always)]
    pub fn insert(&self, key: &<<D as TableDef>::C as TableClass>::Key, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
        self.db.insert(key, value)
    }

    /// Checks if the item exists.
    pub fn insert_item(&self, key: &<<D as TableDef>::C as TableClass>::Key, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
        if try!(self.has_item(key, value)) {
            return Err(lmdb::MdbError::KeyExists);
        }
        self.set(key, value)
    }

    /// Returns true if item exists.
    pub fn has_item(&self, key: &<<D as TableDef>::C as TableClass>::Key, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<bool> {
        let mut cursor = try!(self.new_cursor());
        match cursor.to_item(key, value) {
            Ok(()) => Ok(true),
            Err(lmdb::MdbError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &<<D as TableDef>::C as TableClass>::Key) -> MdbResult<<<D as TableDef>::C as TableClass>::Value> {
        self.db.get(key)
    }

    #[inline(always)]
    pub fn del(&self, key: &<<D as TableDef>::C as TableClass>::Key) -> MdbResult<()> {
        self.db.del(key)
    }

    #[inline(always)]
    pub fn del_item(&self, key: &<<D as TableDef>::C as TableClass>::Key, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
        self.db.del_item(key, value)
    }

    #[inline(always)]
    pub fn new_cursor<'table>(&'table self) -> MdbResult<TypedCursor<'table, D>> {
        self.db.new_cursor().map(|c| TypedCursor {cursor: c, tabledef: PhantomData})
    }
}

/// Is a typed version of lmdb::Cursor.
pub struct TypedCursor<'table, D: TableDef> {
    cursor: Cursor<'table>,
    tabledef: PhantomData<D>,
}

impl<'table, D: TableDef> TypedCursor<'table, D> {
    #[inline(always)]
    pub fn to_first(&mut self) -> MdbResult<()> {
        self.cursor.to_first()
    }

    #[inline(always)]
    pub fn to_last(&mut self) -> MdbResult<()> {
        self.cursor.to_last()
    }

    #[inline(always)]
    pub fn to_key(&mut self, key: &<<D as TableDef>::C as TableClass>::Key) -> MdbResult<()> {
        self.cursor.to_key(key)
    }

    #[inline(always)]
    pub fn to_gte_key(&mut self, key: &<<D as TableDef>::C as TableClass>::Key) -> MdbResult<()> {
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
    pub fn to_item(&mut self, key: &<<D as TableDef>::C as TableClass>::Key, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
        self.cursor.to_item(key, value)
    }

    #[inline(always)]
    pub fn to_gte_item(&mut self, key: &<<D as TableDef>::C as TableClass>::Key, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
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
    pub fn get<'a>(&'a mut self) -> MdbResult<(<<D as TableDef>::C as TableClass>::Key, <<D as TableDef>::C as TableClass>::Value)> /*where D::C::Key: 'a, D::C::Value: 'a*/ {
        self.cursor.get()
    }

    #[inline(always)]
    pub fn get_value<'a>(&'a mut self) -> MdbResult<<<D as TableDef>::C as TableClass>::Value> {
        self.cursor.get_value()
    }

    #[inline(always)]
    pub fn get_key<'a>(&'a mut self) -> MdbResult<<<D as TableDef>::C as TableClass>::Key> {
        self.cursor.get_key()
    }

    #[inline(always)]
    pub fn replace(&mut self, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
        self.cursor.replace(value)
    }

    #[inline(always)]
    pub fn add_item(&mut self, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
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

extern "C" fn sort<T:FromMdbValue+Ord>(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> lmdb::c_int {
    let lhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(lhs_val)});
    let rhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(rhs_val)});

    let order: i8 = unsafe { std::mem::transmute(lhs.cmp(&rhs)) };
    order as lmdb::c_int
}

extern "C" fn sort_reverse<T:FromMdbValue+Ord>(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> lmdb::c_int {
    let lhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(lhs_val)});
    let rhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(rhs_val)});
    let order: i8 = unsafe { std::mem::transmute(lhs.cmp(&rhs).reverse()) };
    order as lmdb::c_int
}

pub mod table_classes {
    use super::{TableClass};
    use super::lmdb::{self, Database, DbFlags, FromMdbValue, ToMdbValue, MdbValue};
    use super::lmdb::core::{MdbResult, DbIntKey, DbAllowDups, DbAllowIntDups, DbDupFixed};
    use super::{sort, sort_reverse};
    use std::mem;
    use std::marker::PhantomData;

    /// 64-bit id type 
    pub type Id64 = u64;

    /// 16-bit index type
    pub type Idx16 = u16;

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
    /// A (Id64, Id64) tuple type
    pub struct Id64x2(pub Id64, pub Id64);

    impl FromMdbValue for Id64x2 {
        #[inline(always)]
        fn from_mdb_value(value: &MdbValue) -> Id64x2 {
            assert!(value.get_size() == mem::size_of::<Id64x2>());
            unsafe {
                let ptr: *const Id64x2 = value.get_ref() as *const Id64x2;
                return *ptr;
            }
        }
    }

    impl ToMdbValue for Id64x2 {
        fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
            unsafe {
                MdbValue::new(mem::transmute(self as *const Id64x2), mem::size_of::<Id64x2>())
            }
        }
    }

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
    pub struct Idx16Id64(pub Idx16, pub Id64);

    impl FromMdbValue for Idx16Id64 {
        #[inline(always)]
        fn from_mdb_value(value: &MdbValue) -> Idx16Id64 {
            assert!(value.get_size() == mem::size_of::<Idx16Id64>());
            unsafe {
                let ptr: *const Idx16Id64 = value.get_ref() as *const Idx16Id64;
                return *ptr;
            }
        }
    }

    impl ToMdbValue for Idx16Id64 {
        fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
            unsafe {
                MdbValue::new(mem::transmute(self as *const Idx16Id64), mem::size_of::<Idx16Id64>())
            }
        }
    }


    /// CREATE TABLE (key: Id64, val: Id64, UNIQUE (key ASC, val ASC))
    pub struct Unique__Id64_Id64;

    impl TableClass for Unique__Id64_Id64 {
        type Key = Id64;
        type Value = Id64;

        fn dbflags() -> DbFlags {
            assert!(mem::size_of::<Self::Key>() == mem::size_of::<usize>());
            assert!(mem::size_of::<Self::Value>() == mem::size_of::<usize>());
            DbIntKey | DbAllowDups | DbAllowIntDups | DbDupFixed
        }

        // Uses default sort order for both key and value
        fn prepare_database(_db: &Database) -> MdbResult<()> { Ok(()) }
    }

    /// CREATE TABLE (key: Id64, val: Id64, UNIQUE (key ASC, val DESC))
    pub struct Unique__Id64_Id64Rev;

    impl TableClass for Unique__Id64_Id64Rev {
        type Key = Id64;
        type Value = Id64;

        fn dbflags() -> DbFlags {
            use ::std::mem;
            assert!(mem::size_of::<Self::Key>() == mem::size_of::<usize>());
            assert!(mem::size_of::<Self::Value>() == mem::size_of::<usize>());
            DbIntKey | DbAllowDups | DbAllowIntDups | DbDupFixed
        }

        // Uses reverse sort order for value, default for key.
        fn prepare_database(db: &Database) -> MdbResult<()> {
            db.set_dupsort(sort_reverse::<Self::Value>)
        }
    }

    /// CREATE TABLE (key: (Id64, Id64), val: Id64, UNIQUE (key ASC, val ASC))
    pub struct Unique__Id64x2_Id64;

    impl TableClass for Unique__Id64x2_Id64 {
        type Key = Id64x2;
        type Value = Id64;

        fn dbflags() -> DbFlags {
            assert!(mem::size_of::<Self::Value>() == mem::size_of::<usize>());
            DbAllowDups | DbAllowIntDups | DbDupFixed
        }

        // Uses default sort order for both key and value
        fn prepare_database(_db: &Database) -> MdbResult<()> { Ok(()) }
    }

    pub type Blob<'a> = &'a [u8];

    /// CREATE TABLE (key: Id64, val: BLOB, UNIQUE (key ASC))
    pub struct UniqueKey__Id64_Blob<'a> {
        marker: PhantomData<&'a()>,
    }

    impl<'a> TableClass for UniqueKey__Id64_Blob<'a> {
        type Key = Id64;
        type Value = Blob<'a>;

        fn dbflags() -> DbFlags {
            assert!(mem::size_of::<Self::Key>() == mem::size_of::<usize>());
            DbIntKey
        }

        // Uses default sort order for both key and value
        fn prepare_database(_db: &Database) -> MdbResult<()> { Ok(()) }
    }



}


#[test]
fn test_simple_table() {
    use std::path::Path;

    let env = lmdb::EnvBuilder::new().max_dbs(2).autocreate_dir(true).open(&Path::new("./test/db1"), 0o777).unwrap();

    table_def!(MyFirstTable, table_classes::Unique__Id64_Id64Rev,             "my_first_table");
    table_def_lifetime!(MyBlobTable, table_classes::UniqueKey__Id64_Blob<'a>, "my_blob_table");

    // A Unique(Id64, Id64 DESC) table
    let table_handle = MyFirstTable::create_table(&env).unwrap();
    let blobs_handle = MyBlobTable::create_table(&env).unwrap();

    // prepare database
    {
        let txn = env.new_transaction().unwrap();
        {
            let table = table_handle.bind_transaction(&txn).unwrap(); 
            let blobs = blobs_handle.bind_transaction(&txn).unwrap();

            let big_blob: &[u8] = b"Test";
            blobs.set(&1, &big_blob);

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
        let table = table_handle.bind_reader(&rdr).unwrap();

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



