# typed-lmdb

This is a thin wrapper around the [Rust binding for LMDB][lmdb-rs] to keep
track of the the types of key/value pairs of a table.

It protects you from accidentially reading (or writing) a differently typed key
or value from (or to) the database, other than that defined in the schema
definition for that database. It also allows you to use a custom key/value
compare function and enforces it being set *before* you can access the
database.

[lmdb-rs]: https://github.com/vhbit/lmdb-rs
