#![feature(associated_consts)]
extern crate lmdb_rs as lmdb;

use lmdb::{FromMdbValue, ToMdbValue, MdbValue, DbFlags, DbHandle, Database, Environment, Transaction, ReadonlyTransaction, Cursor, MDB_val};
use lmdb::core::MdbResult;
use std::marker::PhantomData;

/// For encoding the name of the Table in the type system.
pub trait TableName {
    fn as_str() -> &'static str;
}

#[macro_export]
macro_rules! def_tablename {
    (
        $structname:ident as $str:expr
    ) => {
        #[derive(Clone)]
        struct $structname;
        impl TableName for $structname {
            fn as_str() -> &'static str { $str }
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
    fn prepare_database(db: &Database);

    fn create_table<N: TableName>(env: &Environment) -> MdbResult<TableHandle<Self, N>> {
        env.create_db(N::as_str(), Self::dbflags()).map(|dbh| {
            TableHandle {
                dbh: dbh,
                tableclass: PhantomData,
                tablename: PhantomData
            }
        })
    }

    fn open_table<N: TableName>(env: &Environment) -> MdbResult<TableHandle<Self, N>> {
        env.get_db(N::as_str(), Self::dbflags()).map(|dbh| {
            TableHandle {
                dbh: dbh,
                tableclass: PhantomData,
                tablename: PhantomData
            }
        })
    }
}

/// Wraps a lmdb::DbHandle, keeping the TableClass and TableName in the type signature.
pub struct TableHandle<C: TableClass, N: TableName> {
    dbh: DbHandle,
    tableclass: PhantomData<C>,
    tablename: PhantomData<N>,
}

impl<C: TableClass, N: TableName> TableHandle<C, N> {
    /// Bind a TableHandle towards a transaction. Returns a Table which you can use to
    /// modify the database.
    #[inline(always)]
    pub fn bind_transaction<'txn>(&self, txn: &'txn Transaction) -> Table<'txn, C, N> {
        let db = txn.bind(&self.dbh);
        C::prepare_database(&db);
        Table {
            db: db,
            tableclass: PhantomData,
            tablename: PhantomData
        } 
    }

    /// Bind a TableHandle towards a readonly transaction.
    /// XXX: This should return a ReadonlyTable?
    #[inline(always)]
    pub fn bind_reader<'txn>(&self, txn: &'txn ReadonlyTransaction) -> Table<'txn, C, N> {
        let db = txn.bind(&self.dbh);
        C::prepare_database(&db);
        Table {
            db: db,
            tableclass: PhantomData,
            tablename: PhantomData
        } 
    }

}

/// Wraps a lmdb::Database, keeping the TableClass and TableName in the type signature.
pub struct Table<'db, C: TableClass, N: TableName> {
    db: Database<'db>,
    tableclass: PhantomData<C>,
    tablename: PhantomData<N>,
}

impl<'db, C: TableClass, N: TableName> Table<'db, C, N> {
    #[inline(always)]
    pub fn set(&self, key: &C::Key, value: &C::Value) -> MdbResult<()> {
        self.db.set(key, value)
    }

    #[inline(always)]
    pub fn new_cursor<'table>(&'table self) -> MdbResult<TypedCursor<'table, C, N>> {
        self.db.new_cursor().map(|c| TypedCursor {cursor: c, tableclass: PhantomData, tablename: PhantomData})
    }
}

/// Is a typed version of lmdb::Cursor.
pub struct TypedCursor<'table, C: TableClass, N: TableName> {
    cursor: Cursor<'table>,
    tableclass: PhantomData<C>,
    tablename: PhantomData<N>,
}

impl<'table, C: TableClass, N: TableName> TypedCursor<'table, C, N> {
    #[inline(always)]
    pub fn to_item(&mut self, key: &C::Key, value: &C::Value) -> MdbResult<()> {
        self.cursor.to_item(key, value)
    }

    #[inline(always)]
    pub fn to_next_item(&mut self) -> MdbResult<()> {
        self.cursor.to_next_item()
    }

    #[inline(always)]
    pub fn get<'a>(&'a mut self) -> MdbResult<(C::Key, C::Value)> where C::Key: 'a, C::Value: 'a {
        self.cursor.get()
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

#[test]
fn test_simple_table() {
    use std::path::Path;
    use lmdb::MdbError;

    let env = lmdb::EnvBuilder::new().max_dbs(2).autocreate_dir(true).open(&Path::new("./test/db1"), 0o777).unwrap();

    def_tablename!(MyfirstTable_TableName as "myfirst_table");

    type Id64 = u64;
    struct SortedSet_Id64_Id64Rev;
    impl TableClass for SortedSet_Id64_Id64Rev {
        type Key = Id64;
        type Value = Id64;

        fn dbflags() -> lmdb::DbFlags {
            lmdb::core::DbIntKey | lmdb::core::DbAllowDups | lmdb::core::DbAllowIntDups | lmdb::core::DbDupFixed
        }
        // XXX: pass MdbResult back
        fn prepare_database(db: &lmdb::Database) {
            db.set_dupsort(sort_reverse::<Id64>).unwrap()
        }
    }

    // A Unique(Id64, Id64 DESC) table
    let table_handle = SortedSet_Id64_Id64Rev::create_table::<MyfirstTable_TableName>(&env).unwrap();

    // prepare database
    {
        let txn = env.new_transaction().unwrap();
        {
            let table = table_handle.bind_transaction(&txn); 
            let key = 1u64;
            table.set(&key, &200u64).unwrap();
            table.set(&key, &100u64).unwrap();
            table.set(&key, &2u64).unwrap();
            table.set(&key, &2u64).unwrap();
            table.set(&key, &2u64).unwrap();
            table.set(&key, &2u64).unwrap();
            table.set(&key, &1u64).unwrap();
            table.set(&key, &2u64).unwrap();
            table.set(&key, &3u64).unwrap();
            table.set(&key, &100u64).unwrap();
        }
        txn.commit().unwrap();
    }
    // read
    {
        let rdr = env.get_reader().unwrap();
        let table = table_handle.bind_reader(&rdr);

        let key = 1u64;

/*
        let mut iter = db.item_iter(&key).unwrap();

        let kv = iter.next().map(|c| c.get());
        assert_eq!(Some((1u64, 200u64)), kv);

        let kv = iter.next().map(|c| c.get());
        assert_eq!(Some((1u64, 100u64)), kv);

        let kv = iter.next().map(|c| c.get());
        assert_eq!(Some((1u64, 3u64)), kv);

        let kv = iter.next().map(|c| c.get());
        assert_eq!(Some((1u64, 2u64)), kv);

        let kv = iter.next().map(|c| c.get());
        assert_eq!(Some((1u64, 1u64)), kv);

        let kv: Option<(u64,u64)> = iter.next().map(|c| c.get());
        assert_eq!(None, kv);
*/

        let mut cursor = table.new_cursor().unwrap();
        {
            cursor.to_item(&1u64, &3u64).unwrap();
            assert_eq!((1u64, 3u64), cursor.get().unwrap());

            cursor.to_next_item().unwrap();
            assert_eq!((1u64, 2u64), cursor.get().unwrap());
       
            cursor.to_next_item().unwrap();
            assert_eq!((1u64, 1u64), cursor.get().unwrap());
 
            let err = cursor.to_next_item();
            match err {
                Err(MdbError::NotFound) => assert!(true),
                _ => assert!(false),
            }
        }
    }
}



