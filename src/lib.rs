#![feature(associated_consts)]
extern crate lmdb_rs as lmdb;

use lmdb::{FromMdbValue, ToMdbValue, MdbValue, DbFlags, DbHandle, Database, Environment, Transaction, ReadonlyTransaction, Cursor, MDB_val};
use lmdb::core::MdbResult;
use std::marker::PhantomData;

#[macro_export]
macro_rules! table_def {
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

/// A TableClass describes the types of key/value and the behaviour (sort function) of a Table.
pub trait TableClass: Sized {
    type Key: ToMdbValue + FromMdbValue;
    type Value: ToMdbValue + FromMdbValue;

    /// Flags used for creation/open
    fn dbflags() -> DbFlags;

    /// Is called before accessing a database. Used to setup custom sort functions etc. 
    fn prepare_database(db: &Database);
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
pub struct TableHandle<D: TableDef> {
    dbh: DbHandle,
    tabledef: PhantomData<D>,
}

impl<D: TableDef> TableHandle<D> {
    /// Bind a TableHandle towards a transaction. Returns a Table which you can use to
    /// modify the database.
    #[inline(always)]
    pub fn bind_transaction<'txn>(&self, txn: &'txn Transaction) -> Table<'txn, D> {
        let db = txn.bind(&self.dbh);
        D::C::prepare_database(&db);
        Table {
            db: db,
            tabledef: PhantomData,
        } 
    }

    /// Bind a TableHandle towards a readonly transaction.
    /// XXX: This should return a ReadonlyTable?
    #[inline(always)]
    pub fn bind_reader<'txn>(&self, txn: &'txn ReadonlyTransaction) -> Table<'txn, D> {
        let db = txn.bind(&self.dbh);
        D::C::prepare_database(&db);
        Table {
            db: db,
            tabledef: PhantomData,
        } 
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
    pub fn to_item(&mut self, key: &<<D as TableDef>::C as TableClass>::Key, value: &<<D as TableDef>::C as TableClass>::Value) -> MdbResult<()> {
        self.cursor.to_item(key, value)
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

    table_def!(MyFirstTable, SortedSet_Id64_Id64Rev, "my_first_table");

    // A Unique(Id64, Id64 DESC) table
    let table_handle = MyFirstTable::create_table(&env).unwrap();

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
            let err = cursor.to_item(&2u64, &3u64);
            match err {
                Err(MdbError::NotFound) => assert!(true),
                _ => assert!(false),
            }

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



