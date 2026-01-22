//! # LuhORM
//!
//! A compile-time ORM that generates type-safe database code through build-time introspection.
//! Supporting Sqlite and Postgresql out the box but extensible to another database see [`Backend`] trait
//!
//! ## Features
//!
//! - **Compile-time code generation** - Zero runtime reflection, all code generated at build time
//! - **Type-safe queries** - Builder pattern with compile-time checked columns
//! - **Foreign key relationships** - Automatic join methods and aggregation helpers
//! - **Multiple databases** - Built-in support for SQLite and PostgreSQL
//! - **Migration system** - Hash-verified migrations with automatic tracking
//!
//! ## Quick Start
//!
//! ### Prerequisites
//!
//! - must have rand & chrono in Cargo.toml
//! - must have sqlx
//!
//! 1. Add to `build.rs`:
//! ```no_run
//! use luhorm::Codegen;
//! use sqlx::sqlite::SqlitePoolOptions;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pool = SqlitePoolOptions::new()
//!         .connect("sqlite:my_db.db")
//!         .await
//!         .unwrap();
//!     
//!     Codegen::new("orm", pool, "migrations", None)
//!         .await
//!         .unwrap()
//!         .run_codegen()
//!         .unwrap();
//! }
//! ```
//!
//! 2. Include generated code in `src/main.rs`:
//! ```ignore
//! mod orm {
//!     include!(concat!(env!("OUT_DIR"), "/orm.rs"));
//! }
//! ```
//!
//! 3. Use it:
//! ```ignore
//! use crate::orm::{users::{Users, UsersBuilder}, entry::AggregateEntryUsers};
//!
//! let users = Users::query()
//!     .name("alice")
//!     .age_gt(18)
//!     .fetch_all(&pool)
//!     .await?;
//!
//! let x = Users::query()
//!     // could do this aswell
//!     // .join(Entry::NAME, Users::ID.of(), Entry::USERID.of())
//!     .join_entry()
//!     .fetch_with_entry(&pool)
//!     .await?
//!     .one_to_many()?;
//!
//! // new feature might change
//! let new_user = UsersBuilder::new()
//!     .populate_fake_data()
//!     .build();
//!
//! new_user.insert(&pool).await?;
//! ```
//!
//! ## Current Limitations
//!
//! - Transaction support would be tricky or at least idk I'm still thinking about how I want to do it
//! - Composite keys are a myth
//!
//! > made with love - s.c 2026

mod impls;

use std::{borrow::Cow, collections::HashMap, env, fs, path::{Path, PathBuf}};
use convert_case::{Casing, Case};
use luhtwin::{at, Encase, LuhTwin, Wrap};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// # DbType
///
/// The main abstraction around a type inside introspection of the database
/// can be one of a few options... the key is they all map to rust types through
/// [`DbType::rust_type()`] and [`DbType::cow_rust_type()`] for use in codegen
///
/// ## Provided Methods
///
/// ## `pub fn rust_type(&self) -> &'static str`
///
/// Returns a mapping of a DbType to an equivalent rust type
///
/// ## `pub fn default_value(&self) -> &'static str`
///
/// Generates default values for every value in [`DbType`]
///
/// ## `pub fn requires_import(&self) -> (&'static str, bool)`
///
/// Returns a mapping of a DbType to a path to "use X;" in codegen
/// Just returns the type and false if it is not an imported type
///    
/// ## `pub fn cow_rust_type(&self) -> (&'static str, bool)`
///
/// Returns a mapping of a DbType to an equivalent rust type
/// Also accounting for Cow<...> optimisations in Query codegen structs
#[derive(Eq, PartialEq, PartialOrd, Debug, Hash, Clone)]
pub enum DbType {
    Integer,
    BigInt,
    Real,
    Boolean,
    Text,
    Blob,
    Timestamp,
    Json,
    Numeric,
    Uuid,
    Unknown(String),
}

impl DbType {
    /// Returns a mapping of a DbType to an equivalent rust type
    pub fn rust_type(&self) -> &'static str {
        match self {
            DbType::Integer => "i32",
            DbType::BigInt => "i64",
            DbType::Real => "f64",
            DbType::Boolean => "bool",
            DbType::Text => "String",
            DbType::Blob => "Vec<u8>",
            DbType::Timestamp => "sqlx::types::chrono::NaiveDateTime",
            DbType::Json => "serde_json::Value",
            // damn this is broken we gotta use sqlx::Decimal
            DbType::Numeric => "sqlx::types::Decimal",
            DbType::Uuid => "uuid::Uuid",
            DbType::Unknown(_) => "String",
        }
    }

    /// Generates default values for every value in [`DbType`]
    pub fn default_value(&self) -> &'static str {
        match self {
            DbType::Integer => "0",
            DbType::BigInt => "0",
            DbType::Real => "0.0",
            DbType::Boolean => "false",
            DbType::Text => "\"\".to_string()",
            DbType::Blob => "Vec::new()",
            DbType::Timestamp => "sqlx::types::chrono::NaiveDateTime::from_timestamp_opt(0, 0).unwrap()",
            DbType::Json => "serde_json::Value::Null",
            DbType::Numeric => "sqlx::types::Decimal::ZERO",
            DbType::Uuid => "uuid::Uuid::nil()",
            DbType::Unknown(_) => "\"\".to_string()",
        }
    }

    /// Returns a mapping of a DbType to a path to "use X;" in codegen
    /// Just returns the type and false if it is not an imported type
    pub fn requires_import(&self) -> (&'static str, bool)  {
        let whether = match self {
            DbType::Timestamp => true,
            DbType::Json => true,
            DbType::Numeric => true,
            DbType::Uuid => true,

            _ => false
        };

        (self.rust_type(), whether)
    }
    
    /// Returns a mapping of a DbType to an equivalent rust type
    /// Also accounting for Cow<...> optimisations in Query codegen structs
    pub fn cow_rust_type(&self) -> (&'static str, bool) {
        match self {
            DbType::Text => ("Cow<'a, str>", true),
            DbType::Blob => ("Cow<'a, [u8]>", true),
            _ => (self.rust_type(), false),
        }
    }
}

/// # LuhField
///
/// An abstraction for a value in the columns of [`LuhRow`]
/// it could be extracted to be a string, i64, f64, bool or as raw Vec<u8>
/// it is then passed on to be [`LuhTyped`] which can then be abstracted
/// into a raw value
///
/// ## Provided Methods
///
/// ## `pub fn string(self) -> LuhTyped<'a, String>`
///
/// Extracts value to a [`LuhTyped`] string
///
/// ## `pub fn i64(self) -> LuhTyped<'a, i64>`
///
/// Extracts value to a [`LuhTyped`] i64
///
/// ## `pub fn raw(self) -> LuhTyped<'a, Vec<u8>>`
///
/// Extracts value to a [`LuhTyped`] Vec<u8>
///
/// ## `pub fn f64(self) -> LuhTyped<'a, f64>`
/// Extracts value to a [`LuhTyped`] f64
///
/// ## `pub fn bool(self) -> LuhTyped<'a, bool>`
///
/// Extracts value to a [`LuhTyped`] bool
#[derive(Debug, Clone)]
pub struct LuhField<'a> {
    key: &'a str,
    value: Option<&'a Value>,
}

impl<'a> LuhField<'a> {
    /// Extracts value to a [`LuhTyped`] string
    pub fn string(self) -> LuhTyped<'a, String> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| v.as_str().map(|s| s.to_string()),
        }
    }

    /// Extracts value to a [`LuhTyped`] i64
    pub fn i64(self) -> LuhTyped<'a, i64> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| v.as_i64(),
        }
    }

    /// Extracts value to a [`LuhTyped`] Vec<u8>
    pub fn raw(self) -> LuhTyped<'a, Vec<u8>> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| serde_json::to_vec(v).ok()
        }
    }

    /// Extracts value to a [`LuhTyped`] f64
    pub fn f64(self) -> LuhTyped<'a, f64> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| v.as_f64()
        }
    }

    /// Extracts value to a [`LuhTyped`] bool
    pub fn bool(self) -> LuhTyped<'a, bool> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| v.as_bool(),
        }
    }
}

/// # LuhTyped
///
/// The last step before retrieval of a value from the columns of [`LuhRow`]
/// you either have two paths [`LuhTyped::ok()`] which is a path returning a
/// Option<T> and the other option is [`LuhTypes::require()`] which is a path
/// resulting in [`luhtwin::LuhTwin<T>`] being returned which then can be passed along as a error
///
/// ## Provided Methods
///
/// ## `pub fn ok(&self) -> Option<T>`
///
/// Path resulting in Option<T> being returned
///
/// ## `pub fn require(&self) -> LuhTwin<T>`
///
/// Path resulting in LuhTwin<T> being returned
pub struct LuhTyped<'a, T> {
    key: &'a str,
    value: Option<&'a Value>,
    extract: fn(&Value) -> Option<T>,
}

impl<'a, T> LuhTyped<'a, T> {
    /// Path resulting in Option<T> being returned
    pub fn ok(&self) -> Option<T> {
        self.value.and_then(|v| (self.extract)(v))
    }

    /// Path resulting in LuhTwin<T> being returned
    pub fn require(&self) -> LuhTwin<T> {
        self.ok().ok_or_else(|| {
            at!("column '{}' missing or wrong type", self.key).into()
        })
    }
}

/// # LuhRow
///
/// The main abstraction around rows in LuhOrm
/// it is very simple by design
///
/// ## Provided Methods
///
/// ## `pub fn new() -> Self`
///
/// Creates a new [`LuhRow`] with empty columns
///
/// ## `pub fn from_hashmaps(value: impl IntoIterator<Item = HashMap<String, Value>>) -> Vec<Self>`
///
/// Generates a Vec of [`LuhRows`] from a list of arbitrary hashmaps
///
/// ## `pub fn value<'a>(&'a self, key: &'a str) -> LuhField<'a>`
///
/// Returns a [`LuhField`] which allows you retrieve the data found inside the column
///
/// ### Notes
///
/// See [`LuhField`] for more details (useful if you are implementing [`Backend`] for a new database)
///
/// ## `pub fn insert<S: Into<String>>(&mut self, key: S, value: Value)`
/// Just inserts a value into the map (boring)
///
/// ## Notes
///
/// If you are implementing your own database type to work within LuhOrm
/// you will have to define the From<> trait for your databases row into LuhRow
#[derive(Debug, Clone)]
pub struct LuhRow {
    cols: HashMap<String, Value>,
}

impl LuhRow {
    /// Creates a new [`LuhRow`] with empty columns
    pub fn new() -> Self {
        Self { cols: HashMap::new() }
    }

    /// Generates a Vec of [`LuhRows`] from a list of arbitrary hashmaps
    pub fn from_hashmaps(value: impl IntoIterator<Item = HashMap<String, Value>>) -> Vec<Self> {
        value
            .into_iter()
            .map(LuhRow::from)
            .collect()
    }

    /// Returns a [`LuhField`] which allows you retrieve the data found inside the column
    ///
    /// ## Notes
    ///
    /// See [`LuhField`] for more details (useful if you are implementing [`Backend`] for a new database)
    pub fn value<'a>(&'a self, key: &'a str) -> LuhField<'a> {
        LuhField {
            key,
            value: self.cols.get(key),
        }
    }

    /// Just inserts a value into the map (boring)
    pub fn insert<S: Into<String>>(&mut self, key: S, value: Value) {
        self.cols.insert(key.into(), value);
    }
}

impl From<HashMap<String, Value>> for LuhRow {
    fn from(value: HashMap<String, Value>) -> Self {
        Self { cols: value }
    }
}

/// # ForeignKey
///
/// This is a core abstraction of a foreign key in a table
/// This is required by [`Codegen`] to do some of its job.
#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub is_return: bool,

    pub table: String,
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

/// # ColumnData
///
/// This is a core abstraction of a database column
/// It carries all information related to columns in
/// any given database that is required needed by [`Codegen`]
///  
/// ## Provided Methods
///
/// ## `fn is_numeric(&self) -> bool`
///
/// Declares what [`DbType`]s will have gt, lt, gte, lte methods are
/// generated for them
///
/// ## `fn is_string_like(&self) -> bool`
///
/// Declared what types will be used with Cow in a given Query struct
/// in codegen
/// Long story short this does not matter to you
#[derive(Debug, Clone)]
pub struct ColumnData {
    name: String,
    kind: DbType,
    field_name: String,
    not_null: i64,
}

impl ColumnData {
    /// Declares what [`DbType`]s will have gt, lt, gte, lte methods are
    /// generated for them
    fn is_numeric(&self) -> bool {
        matches!(
            self.kind,
            DbType::Integer | DbType::BigInt | DbType::Real | DbType::Numeric
        )
    }

    /// Declared what types will be used with Cow in a given Query struct
    /// in codegen
    ///
    /// ## Notes
    ///
    /// Long story short this does not matter to you
    fn is_string_like(&self) -> bool {
        matches!(
            self.kind,
            DbType::Text | DbType::Blob
        )
    }
}

/// # TableData
///
/// This is a core abstraction of a database table
/// This contains everything [`Codegen`] needs to do its job
pub struct TableData {
    pub cols: Vec<ColumnData>,
    pub fks: Vec<ForeignKey>,
    pub name: String
}

/// Just helpers for stripping parens from sql types
pub trait StripParens {
    /// Just a helper function for strip parenthesis out of sql types
    ///
    /// Note: if your looking at the implementation of this function
    /// thinking why do you split_whitespace then join(" ") well
    /// sql types can be multiple words if so we want to respect that
    /// while still stripping parenthesis and that is your answer to why I did that
    fn sql_strip_parens(&self) -> String;
}

impl StripParens for &str {
    fn sql_strip_parens(&self) -> String {
        self.split('(').next().unwrap_or(self)
            .trim()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Just helpers for normalising sql types
pub trait SqlNormalise {
    fn sql_normalise(&self) -> String;
}

impl SqlNormalise for &str {
    fn sql_normalise(&self) -> String {
        self.sql_strip_parens()
            .trim()
            .to_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>() .join(" ")
    }
}

/// # TypeMapper
///
/// The basic type related things required for [`Backend`] and [`Codegen`]
/// to be able to function... we already define [`PostgresMapper`] and [`SqliteMapper`]
/// for you any more DBs you will need to add yourself through fork or ask
/// me to implement them...
///
/// ## Expected Methods
///
/// ## `fn create_migration_table(&self) -> &str`
///
/// This is a function which is expected to return SQL in which
/// a table is to be created and ignored if existing...
/// that table is to have 4 columns...
///
/// - `id` --> integer (primary key)
/// - `filename` --> text (not null, unique)
/// - `hash` --> text (not null)
/// - `applied_at` - timestamp (not null, default should be current timestamp that is expected)
///
/// ## `fn split_sql_statements(sql: &str) -> Vec<String>`
/// This function expects you to parse a sql migration into
/// separate statements based of the given sql dialect
/// to be run by [`migrate()`] function
///
/// ## `fn master_table_columns_from_rows<'a>(rows: impl IntoIterator<Item = &'a LuhRow>) -> LuhTwin<Vec<ColumnData>>`
///
/// This function is expected to take LuhRows gathered during
/// [`Backend::get_master_table()`] and parse them into [`ColumnData`]
///
/// ## `fn foreign_keys_from_rows<'a>(table: &str, rows: impl IntoIterator<Item = &'a LuhRow>) -> LuhTwin<Vec<ForeignKey>>`
///
/// This function is expected to take LuhRows gathered during
/// [`Backend::get_master_table()`] and parse them into [`ForeignKey`]
///
/// ## `fn parse_type(decl: &str) -> DbType`
///
/// This function is expected to parse an arbitrary declaration into a [`DbType`]
///
/// ## `fn placeholder(&self, idx: usize) -> String;`
///
/// This function is expected to return the necessary placeholder syntax for this Platform
///
/// ## `fn uses_numbered_placeholders(&self) -> bool`
/// This function is expected to return whether or not idx had to be used in [`TypeMapper::placeholder()`]
///
/// ## `fn sqlx_type(&self) -> String`
///
/// This function is expected to return the sqlx pool type for given platform
/// e.g PgPool or SqlitePool
///
/// ## `fn sqlx_row_type(&self) -> String`
/// 
/// This function is expected to return the sqlx row type for given database
/// e.g postgres::PgRow or sqlite::SqliteRow
pub trait TypeMapper {
    /// This is a function which is expected to return SQL in which
    /// a table is to be created and ignored if existing...
    /// that table is to have 4 columns...
    ///
    /// - `id` --> integer (primary key)
    /// - `filename` --> text (not null, unique)
    /// - `hash` --> text (not null)
    /// - `applied_at` - timestamp (not null, default should be current timestamp that is expected)
    ///
    /// ## Notes
    ///
    /// This does have a default implementation for Sqlite only
    fn create_migration_table(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS _luhorm_migrations (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             filename TEXT NOT NULL UNIQUE,
             hash TEXT NOT NULL,
             applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
         );"
    }

    /// This function expects you to parse a sql migration into
    /// separate statements based of the given sql dialect
    /// to be run by [`migrate()`] function
    fn split_sql_statements(&self, sql: &str) -> LuhTwin<Vec<String>>;

    /// This function is expected to take LuhRows gathered during
    /// [`Backend::get_master_table()`] and parse them into [`ColumnData`]
    ///
    /// ## Notes
    ///
    /// I shouldn't have to say this but if you are writing your own implementation of this
    /// this will take collaboration with [`Backend`] and your specific DB for how that data
    /// will be formatted in those tables
    fn master_table_columns_from_rows<'a>(rows: impl IntoIterator<Item = &'a LuhRow>) -> LuhTwin<Vec<ColumnData>>;

    /// This function is expected to take LuhRows gathered during
    /// [`Backend::get_master_table()`] and parse them into [`ForeignKey`]
    ///
    /// ## Notes
    ///
    /// I shouldn't have to say this but if you are writing your own implementation of this
    /// this will take collaboration with [`Backend`] and your specific DB for how that data
    /// will be formatted in those tables
    fn foreign_keys_from_rows<'a>(
        table: &str,
        rows: impl IntoIterator<Item = &'a LuhRow>
    ) -> LuhTwin<Vec<ForeignKey>>;

    /// This function is expected to parse an arbitrary declaration into a [`DbType`]
    fn parse_type(decl: &str) -> DbType;

    /// This function is expected to return the necessary placeholder syntax for this Platform
    fn placeholder(&self, idx: usize) -> String;

    /// This function is expected to return whether or not idx had to be used in [`TypeMapper::placeholder()`]
    fn uses_numbered_placeholders(&self) -> bool;

    /// This function is expected to return the sqlx pool type for given database
    /// e.g PgPool or SqlitePool
    ///
    /// ## Notes
    ///
    /// this is used for codegen
    fn sqlx_type(&self) -> String;

    /// This function is expected to return the sqlx row type for given database
    /// e.g postgres::PgRow or sqlite::SqliteRow
    ///
    /// ## Notes
    ///
    /// this is used for codegen
    fn sqlx_row_type(&self) -> String;
}

/// # LuhParam
///
/// Just a basic enum for wrapping different types to be binded
pub enum LuhParam<'a> {
    I64(i64),
    F64(f64),
    String(&'a str),
    Bool(bool),
}

/// Just an alias for lifetime management
type Params<'a> = Option<&'a [LuhParam<'a>]>;

/// # Backend
///
/// This is main abstraction around DBs at codegen in LuhOrm
/// Any type you would want to pass into [`Codegen`] has to implement this trait
/// [`sqlx::Pool<Sqlite>`] and [`sqlx::Pool<Postgres>`] implementations have been given for you
///
/// ## Expected Types
///
/// ## `type TM: TypeMapper`
///
/// This must be set to this DB's related type mapper
///
/// ## Expected Methods
///
/// ## `fn type_mapper(&self) -> &Self::TM`
///
/// This is expected to return the related type mapper with this DB type
///
/// ## `async fn execute_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<u64>`
///
/// This function is expected to exectute a raw query and respect params
/// if they were set
///
/// ## `async fn fetch_all_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<Vec<LuhRow>>`
///
/// This function is expected to fetch a raw query and respect params
/// if they were set and then return a Vec<LuhRow>
///
/// ## `async fn get_master_table(&self) -> LuhTwin<Vec<TableData>>`
///
/// This function is expected to introspect the DB and then return a
/// a Vec of [`TableData`]
///
/// NOTE: if you are writing your own implementation of this for a new database type
/// if you want foreign keys to be bidirectional then you must duplicate foreign keys
/// and add them not only to the TableData of which the foreign key belongs to
/// but also to foreign key's table it references (ref_table) in order for bidirectional functionality to work
/// it also expects the foreign key's is_return boolean to be turned to true when that key is duplicated
#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    /// This is just the type of what is to be returned by
    /// [`Backend::type_mapper()`]
    type TM: TypeMapper;

    /// This is expected to return the related type mapper with this DB type
    fn type_mapper(&self) -> &Self::TM;

    /// This function is expected to exectute a raw query and respect params
    /// if they were set
    async fn execute_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<u64>;

    /// This function is expected to fetch a raw query and respect params
    /// if they were set and then return a Vec<LuhRow>
    async fn fetch_all_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<Vec<LuhRow>>;

    /// This function is expected to introspect the DB and then return a
    /// a Vec of [`TableData`]
    ///
    /// NOTE: if you are writing your own implementation of this for a new database type
    /// if you want foreign keys to be bidirectional then you must duplicate foreign keys
    /// and add them not only to the TableData of which the foreign key belongs to
    /// but also to foreign key's table it references (ref_table) in order for bidirectional functionality to work
    /// it also expects the foreign key's is_return boolean to be turned to true when that key is duplicated
    async fn get_master_table(&self) -> LuhTwin<Vec<TableData>>;
}

/// # migrate
///
/// This function is a basic migrate system which must be ran at compile time
/// to keep the database up to the current state of migrations before any
/// introspection happens
///
/// ## Notes
///
/// This is NOT the best migration function but it works if you have any issues
/// please raise a github issue and I will carefully consider what you say
pub async fn migrate<D: Backend>(pool: &D, migr_dir: impl Into<PathBuf>) -> LuhTwin<()> {
    let migr_dir: PathBuf = migr_dir.into();

    let create_table_sql = pool.type_mapper().create_migration_table();
    
    pool.execute_raw(create_table_sql, None)
        .await
        .wrap(|| "failed to run migrations table schema sql")?;

    let applied: Vec<(String, String)> = pool
        .fetch_all_raw("SELECT filename, hash FROM _luhorm_migrations ORDER BY id", None)
        .await
        .wrap(|| "failed to query the pool for applied migrations")?
        .into_iter()
        .map(|row| {
            let filename = row.value("filename").string().require()
                .wrap(|| "failed to get filename")?;
            let hash = row.value("hash").string().require()
                .wrap(|| "failed to get hash")?;
            Ok((filename, hash))
        })
        .collect::<LuhTwin<Vec<_>>>()?;

    let mut entries: Vec<_> = std::fs::read_dir(&migr_dir)
        .wrap(|| "failed to read migrations dir")?
        .filter_map(Result::ok)
        .filter(|e| {
            let path = e.path();

            if path.extension().map(|ext| ext != "sql").unwrap_or(true) {
                return false;
            }

            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if fname.starts_with('.') || fname.starts_with('#') || fname.ends_with('~') {
                    return false;
                }
            }
            true
        })
        .collect();
    
    entries.sort_by_key(|e| e.file_name());
    
    // run checks on old migrations
    for (i, (applied_file, applied_hash)) in applied.iter().enumerate() {
        if let Some(entry) = entries.get(i) {
            let expected_file = entry.file_name().to_string_lossy().to_string();
            
            if applied_file != &expected_file {
                return Err(at!(
                    "migration order mismatch! applied: {}, expected: {}",
                    applied_file, expected_file
                ).into());
            }
            
            let sql = std::fs::read(entry.path())
                .wrap(|| format!("failed to read migration file {}", expected_file))?;
            
            let mut hasher = Sha256::new();
            hasher.update(sql);

            let file_hash = format!("{:x}", hasher.finalize());
            
            if &file_hash != applied_hash {
                return Err(at!(
                    "migration file changed after being applied: {}",
                    expected_file
                ).into());
            }
        } else {
            return Err(at!(
                "database has more applied migrations than files on disk. extra: {}",
                applied_file
            ).into());
        }
    }

    // apply new migrations
    for entry in entries.iter().skip(applied.len()) {
        let filename = entry.file_name().to_string_lossy().to_string();
        let sql_bytes = std::fs::read(entry.path())
            .wrap(|| format!("failed to read migration file {}", filename))?;

        let mut hasher = Sha256::new();
        hasher.update(&sql_bytes);
        let file_hash = format!("{:x}", hasher.finalize());
        
        let sql_str = String::from_utf8(sql_bytes)
            .wrap(|| format!("failed to convert {} to UTF-8", filename))?;
        
        let statements = pool.type_mapper().split_sql_statements(&sql_str)?;
        
        for (idx, statement) in statements.iter().enumerate() {
            pool.execute_raw(statement, None)
                .await
                .wrap(|| format!("failed to execute migration: {} (statement {})", filename, idx + 1))?;
        }

        let kind = pool.type_mapper();

        pool.execute_raw(
            &format!("INSERT INTO _luhorm_migrations (filename, hash) VALUES ({}, {})", 
                kind.placeholder(0), kind.placeholder(1)),
            Some(&[
                LuhParam::String(&filename),
                LuhParam::String(&file_hash)
            ])
        ).await.wrap(|| "failed to insert new migration to table")?;

        println!("applied migration: {}", filename);
    }

    Ok(())
}

/// # RustStringView
///
/// Just a string view for generating rust code
/// it is used only for codegen and its just ease of use
///
/// provided methods is pointless since you will never have to use this
#[derive(Debug, Default)]
pub struct RustStringView {
    buf: String,
    name: String,
    indent: usize,
    indent_with: &'static str,
}

impl RustStringView {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            buf: String::new(),
            name: name.into(),
            indent: 0,
            indent_with: "    ", // 4 spaces
        }
    }

    pub fn doc_header(&mut self, s: impl AsRef<str>) -> &mut Self {
        self.line(format!("//! {}", s.as_ref()))
    }

    pub fn doc_header_blank(&mut self) -> &mut Self {
        self.line("//!")
    }

    pub fn doc(&mut self, s: impl AsRef<str>) -> &mut Self {
        self.line(format!("/// {}", s.as_ref()))
    }

    pub fn doc_blank(&mut self) -> &mut Self {
        self.line("///")
    }

    pub fn indent_with(mut self, s: &'static str) -> Self {
        self.indent_with = s;
        self
    }

    pub fn into_string(self) -> String {
        self.buf
    }

    pub fn write<S: AsRef<str>>(&mut self, s: S) -> &mut Self {
        self.buf.push_str(s.as_ref());
        self
    }

    pub fn line<S: AsRef<str>>(&mut self, s: S) -> &mut Self {
        for _ in 0..self.indent {
            self.buf.push_str(self.indent_with);
        }
        self.buf.push_str(s.as_ref());
        self.buf.push('\n');
        self
    }

    pub fn blank(&mut self) -> &mut Self {
        self.buf.push('\n');
        self
    }

    pub fn indent(&mut self) -> &mut Self {
        self.indent += 1;
        self
    }

    pub fn dedent(&mut self) -> &mut Self {
        if self.indent > 0 {
            self.indent -= 1;
        }
        self
    }
    
    pub fn block<F>(&mut self, header: impl Into<Cow<'static, str>>, f: F) -> &mut Self
    where
        F: FnOnce(&mut Self),
    {
        self.line(header.into());
        self.indent();
        f(self);
        self.dedent();
        self.line("}");
        self
    }

    pub fn block_with_paren<F>(&mut self, header: impl Into<Cow<'static, str>>, f: F) -> &mut Self
    where
        F: FnOnce(&mut Self),
    {
        self.line(header.into());
        self.indent();
        f(self);
        self.dedent();
        self.line("})");
        self
    }
}

/// macro just used for codegen
/// just passes args to format
/// and wraps result in a [`Cow::Owned`]
macro_rules! f {
    ($($args:tt)*) => {
        Cow::Owned(format!($($args)*))
    };
}

/// # Import
///
/// A struct to represents a neccesary import that must happen at codegen
/// this happens because not all types in a given database can be fully
/// represented using just rust types alone... meaning that sometimes we
/// must import other types from other libraries to represent database types.
///
/// We have this struct so we can track them in a list in [`TableState`]
/// to effectively prompt the user of said imports
///
/// ## Provided Methods
///
/// ## `fn from_dbtype(value: (&'static str, bool)) -> Self`
///
/// An easy way to take the result of a [`DbType::requires_import()`] into a [`Import`]
#[allow(dead_code)]
pub struct Import {
    global: bool,
    path: String,
}

impl Import {
    /// An easy way to take the result of a [`DbType::requires_import()`] into a [`Import`]
    fn from_dbtype(value: (&'static str, bool)) -> Self {
        Self {
            path: value.0.to_string(),
            global: value.1
        }
    }
}

/// # ForeignKeyState
///
/// Just contains maps for function names to be defined in codegen (per foreign key)
pub struct ForeignKeyState {
    pub join_method: String,
    pub left_join_method: String,
    pub right_join_method: String,
    pub one_to_many_name: String,
    pub many_to_one_name: String,
    pub trait_name: String,
    pub row_name: String,
    pub fetch_func_name: String,
}

/// # GeneralTableState
///
/// As you can see here this a state for a table
/// It contains things like fields for select sql queries
/// field names, those same fields names but to snake and pascal case,
/// placeholders for your given database syntax, fields for update queries
/// etc...
/// it also contains structure names for struct that will be created in codegen
pub struct GeneralTableState {
    pub select_fields: String,
    pub field_names: Vec<String>,
    pub field_list: String,
    pub rust_field_names: Vec<String>,
    pub placeholders: String,
    pub update_fields: String,
    pub id_type: String,

    pub base_struct_name: String,
    pub column_struct_name: String,
    pub table_struct_name: String,
    pub query_struct_name: String,
    pub builder_struct_name: String
}

/// # TableState
///
/// Carries all configurable options for codegen
/// Tread carefully here because I might say "configurable"
/// But not all codegen is respecting that fully
pub struct TableState {
    pub imports: Vec<Import>,
    pub fks: Vec<ForeignKeyState>,
    pub general: GeneralTableState
}

impl TableState {
    /// Just returns a new GeneralTableState with an empty imports an empty foreignkey state vectors
    pub fn new(general: GeneralTableState) -> Self {
        Self {
            imports: Vec::new(),
            fks: Vec::new(),
            general,
        }
    }
}

/// Just a basic helper struct that turns a non plural word into a plural
/// this helps in making aggregate structs in codegen
trait Pluralise {
    /// Just a basic helper function that turns a non plural word into a plural
    fn pluralise(&self) -> String;
}

impl Pluralise for String {
    fn pluralise(&self) -> String {
        if self.ends_with('y') && self.len() > 1 && !matches!(self.chars().nth(self.len()-2).unwrap(), 'a'|'e'|'i'|'o'|'u') {
            let base = &self[..self.len()-1];
            format!("{}ies", base)
        } else if self.ends_with("s") || self.ends_with("sh") || self.ends_with("ch") || self.ends_with("x") || self.ends_with("z") {
            format!("{}es", self)
        } else {
            format!("{}s", self)
        }
    }
}

/// Just a basic helper struct that turns a plural word into a non plural
/// this helps in making aggregate structs in codegen
trait Unpluralise {
    /// Just a basic helper function that turns a plural word into a non plural
    fn unpluralise(&self) -> String;
}

impl Unpluralise for String {
    fn unpluralise(&self) -> String {
        let x = self.to_lowercase();

        if x.ends_with("ies") && x.len() > 3 {
            let base = &x[..x.len() - 3];
            return format!("{}y", base);
        }

        else if x.ends_with("es") {
            let base = &x[..x.len() - 2];
            let last_two = &base[base.len().saturating_sub(2)..];
            if last_two.ends_with('s') || last_two.ends_with("sh") || last_two.ends_with("ch") || last_two.ends_with('x') || last_two.ends_with('z') {
                return base.to_string();
            }
        }

        else if x.ends_with('s') && x.len() > 1 {
            return x[..x.len()-1].to_string();
        }

        x
    }
}

/// # PatternBuilder
///
/// An easy way to make fake data for your database
/// Very simple in the way it works it only runs at compile time
/// not too much on this because i only really use it myself tbh
pub struct PatternBuilder {
    // default generators per type
    type_patterns: HashMap<DbType, Box<dyn Fn(&mut RustStringView, &ColumnData) -> String + Send + Sync>>,

    // overrides per table/column
    column_patterns: HashMap<(String, String), Box<dyn Fn(&mut RustStringView, &ColumnData) -> String + Send + Sync>>,
}

impl PatternBuilder {
    pub fn new() -> Self {
        Self {
            type_patterns: HashMap::new(),
            column_patterns: HashMap::new(),
        }
    }

    /// Default patterns
    pub fn default_patterns(&self, sv: &mut RustStringView, c: &ColumnData) -> Option<String> {
        let rng_name = format!("{}_rng", c.field_name);
        let idx_name = format!("{}_idx", c.field_name);

        
        if (c.name == "user" || c.name == "username") && c.kind == DbType::Text {
            sv.line("let usernames = [\"wasabi_enjoyer_\", \"jeremy_kyle_lover_\", \"spicy_enthusiast_\", \"toobing_to_the_shop_\", \"quake_3_worshipper_\", \"f3ardr0pped_\", \"sc0r3d4t_\", \"tap3d4t_\", \"l2sc00mth4t_\", \"andwhatelse_\"];");
            sv.line(f!("let mut {} = rand::rng();", rng_name));
            sv.line(f!("let {} = {}.random_range(0..usernames.len());", idx_name, rng_name));
            return Some(format!("format!(\"{{}}_{{}}\", usernames[{}], rand::random::<u16>())", idx_name))
        }
        
        else if (c.name == "password" || c.name == "pass") && c.kind == DbType::Text {
            sv.line("use rand::Rng;");

            return Some("rand::rng().sample_iter(&rand::distr::Alphanumeric).take(12).map(char::from).collect()".into())
        }
        
        else if c.name == "email" && c.kind == DbType::Text {
            sv.line("let domains = [\"toilet.com\", \"bine.com\", \"example.com\", \"creasedat.com\", \"oopsgotyou.org\", \"loudpack.gov\", \"bounceout.dev\", \"yfidat.org\"];");
            
            sv.line(f!("let mut {} = rand::rng();", rng_name));
            sv.line(f!("let {} = rng.random_range(0..domains.len());", idx_name));
            
            return Some(format!("format!(\"user{{}}@{{}}\", rand::random::<u16>(), domains[{}])", idx_name))
        }
        
        else if c.name == "phone" && c.kind == DbType::Text {
            return Some("format!(\"+447{}\", (0..9).map(|_| rand::random::<u8>() % 10).collect::<Vec<_>>().iter().map(|d| d.to_string()).collect::<String>())".to_string())
        }
        
        else if (c.name.starts_with("at_") || c.name.ends_with("_at")) && (c.kind == DbType::Timestamp) {
            sv.line(f!("let {}_days_ago = rand::random::<i64>() % 3650;", c.field_name));
            return Some(format!("(chrono::Utc::now() - chrono::Duration::days({}_days_ago)).date_naive()", c.field_name))
        }
        
        else if (c.name.starts_with("at_") || c.name.ends_with("_at") || c.name.starts_with("date")) && c.kind == DbType::Integer || c.kind == DbType::BigInt {
            sv.line(f!("let {}_days_ago = rand::random::<i64>() % 3650;", c.field_name));
            sv.line(f!("let {}_seconds_ago = {}_days_ago as i64 * 24 * 60 * 60;", c.field_name, c.field_name));
            sv.line(f!("let {}_ts = chrono::Utc::now() - chrono::Duration::seconds({}_seconds_ago);", c.field_name, c.field_name));

            return Some(format!("{}_ts.timestamp() as i32", c.field_name))
        }
        
        else if c.name.starts_with("is_") && c.kind == DbType::Boolean {
            return Some("rand::random::<bool>()".to_string())
        }

        
        else if c.name == "first_name" && c.kind == DbType::Text {
            sv.line("let first_names = [\"Alice\", \"Bob\", \"Charlie\", \"Lewis\", \"Kyle\", \"Amelia\", \"Noah\", \"Liam\", \"Sophia\", \"Isabella\", \"Michael\", \"Mary\", \"Pali\", \"Oliver\"];");
            sv.line(f!("let mut {} = rand::rng();", rng_name));
            sv.line(f!("let {} = rng.random_range(0..first_names.len());", idx_name));
            return Some(format!("first_names[{}].to_string()", idx_name))
        }
        
        else if c.name == "last_name" && c.kind == DbType::Text {
            sv.line("let last_names = [\"Smith\", \"Jones\", \"Taylor\", \"Miller\", \"Davis\", \"Brown\", \"Johnson\", \"Thompson\", \"Lee\", \"Harris\", \"Young\", \"Flores\", \"Drome\", \"Hamilton\"];");
            sv.line(f!("let mut {} = rand::rng();", rng_name));
            sv.line(f!("let {} = rng.random_range(0..last_names.len());", idx_name));
            return Some(format!("last_names[{}].to_string()", idx_name))
        }
        
        else if (c.name == "name" || c.name == "full_name") && c.kind == DbType::Text {
            sv.line("let first_names = [\"Alice\", \"Bob\", \"Charlie\", \"Lewis\", \"Kyle\", \"Amelia\", \"Noah\", \"Liam\", \"Sophia\", \"Isabella\", \"Michael\", \"Mary\", \"Pali\", \"Oliver\"];");
            sv.line("let last_names = [\"Smith\", \"Jones\", \"Taylor\", \"Miller\", \"Davis\", \"Brown\", \"Johnson\", \"Thompson\", \"Lee\", \"Harris\", \"Young\", \"Flores\", \"Drome\", \"Hamilton\"];");

            sv.line(f!("let mut {}_first_idx = rand::rng();", c.field_name));
            sv.line(f!("let {}_last_idx = rng.random_range(0..last_names.len());", c.field_name));
            
            return Some(format!("format!(\"{{}} {{}}\", last_names[{}_first_idx], first_names[{}_last_idx])", c.field_name, c.field_name))
        }
        
        else if c.kind == DbType::Text {
            return Some(format!("\"fake {}\".to_string()", c.field_name))
        }

        None
    }

    /// set default for a specific [`DbType`]
    ///
    /// in the function passed in... the first arguement is the [`RustStringview`] and
    /// the second arguement is [`ColumnData`] for the name of the column field_name etc...
    /// in the string view should be everything you would want to be placed before the final string (this could be something like)
    ///
    /// ```ignore
    /// sv.line("let last_names = [\"Smith\", \"Jones\", \"Taylor\"");
    /// sv.line(f!("let mut {}_rng = rand::rng();", c.field_name));
    /// sv.line(f!("let {}_idx = {}_rng.random_range(0..last_names.len());", c.field_name, c.field_name));
    /// return Some(format!("last_names[{}_idx].to_string()", c.field_name))
    /// ```
    pub fn set_type_pattern<F>(&mut self, ty: DbType, f: F)
    where
        F: Fn(&mut RustStringView, &ColumnData) -> String + 'static + Send + Sync,
    {
        self.type_patterns.insert(ty, Box::new(f));
    }

    /// override pattern for a specific column
    ///
    /// in the function passed in... the first arguement is the [`RustStringview`] and
    /// the second arguement is [`ColumnData`] for the name of the column field_name etc...
    /// in the string view should be everything you would want to be placed before the final string (this could be something like)
    ///
    /// ```ignore
    /// sv.line("let last_names = [\"Smith\", \"Jones\", \"Taylor\"");
    /// sv.line(f!("let mut {}_rng = rand::rng();", c.field_name));
    /// sv.line(f!("let {}_idx = {}_rng.random_range(0..last_names.len());", c.field_name, c.field_name));
    /// return Some(format!("last_names[{}_idx].to_string()", c.field_name))
    /// ```
    pub fn set_column_pattern<F>(&mut self, table: &str, column: &str, f: F)
    where
        F: Fn(&mut RustStringView, &ColumnData) -> String + 'static + Send + Sync,
    {
         self.column_patterns
            .insert((table.to_string(), column.to_string()), Box::new(f));
    }

    /// generate value for a given column/type
    pub fn generate(&self, table: &str, col: &ColumnData, sv: &mut RustStringView) -> String {
        if let Some(f) = self.column_patterns.get(&(table.to_string(), col.name.to_string())) {
            return f(sv, col);
        }

        if let Some(f) = self.type_patterns.get(&col.kind) {
            return f(sv, col);
        }

        // fallback default if no pattern set

        let def_val = self.default_patterns(sv, col);

        def_val.unwrap_or_else(|| match col.kind {
            DbType::Integer => "rand::random::<i32>().abs() % 1000".to_string(),
            DbType::BigInt => "rand::random::<i64>()".to_string(),
            DbType::Real => "rand::random::<f64>()".to_string(),
		    DbType::Boolean => "rand::random::<bool>()".to_string(),
            DbType::Text => "format!(\"test ({})\", (rand::random::<i32>().to_string()))".to_string(),
            DbType::Blob => {
                sv.line(f!("let {}_len = rand::random::<u8>() % 8 + 1;", col.field_name));
                format!("(0..{}_len).map(|_| rand::random::<u8>()).collect::<Vec<u8>>()", col.field_name)
            }
            DbType::Timestamp => "(chrono::Utc::now() - chrono::Duration:days(rand::random::<i64>().abs() % 365)).date_naive()".to_string(),
		    DbType::Json => format!("serde_json::json!({{\"fake\": \"{}\"}})", col.name),
		    DbType::Numeric => "sqlx::types::Decimal::new((rand::random::<i32>().abs() % 500), 2)".to_string(),
            DbType::Uuid => "uuid::Uuid::new_v4()".to_string(),
            _ => col.kind.default_value().to_string(),
        })
    }
}

/// # Codegen
///
/// Arguably the main struct in this crate
/// it handles creating of codegen based of data
/// gathered through the introspection of the database
/// it outputs finished codegen to env::var("OUT_DIR")/orm.rs
/// OUT_DIR is set at compile time
///
/// ## Provided Methods
///
/// ## `pub async fn new(pool: D, migr_dir: impl Into<PathBuf>) -> LuhTwin<Self>`
///
/// Creates a new codegen struct based of a pool which implements [`Backend`]
/// it will also fill out tables and states for you based off pool
///
/// ## `pub fn run_codegen_for_tables(&self) -> LuhTwin<Vec<RustStringView>>`
///
/// Run the codegen for all tables in self.tables
///
/// ## `pub async fn run_codegen(&self) -> LuhTwin<()>`
///
/// Runs the codegen
///
/// ## Notes
///
/// This function outputs finished codegen to env::var("OUT_DIR")/orm.rs
/// OUT_DIR is set at compile time
pub struct Codegen<D: Backend> {
    pool: D,
    crate_name: String,
    tables: Vec<TableData>,
    states: Vec<TableState>,
    fake_data: PatternBuilder,
}

impl<D: Backend> Codegen<D> {
    /// Creates a new codegen struct based of a pool which implements [`Backend`]
    /// it will also fill out tables and states for you based off pool
    pub async fn new(crate_name: impl Into<String>, pool: D, migr_dir: impl Into<PathBuf>, fake_data_generator: Option<PatternBuilder>) -> LuhTwin<Self> {
        let migr_dir: PathBuf = migr_dir.into();

        if !migr_dir.exists() && !migr_dir.is_dir() {
            return Err(at!("migr dir doesn't exist or is a directory: {}", migr_dir.display()).into())
        }

        migrate(&pool, migr_dir)
            .await
            .encase(|| "failed to migrate when making new LuhOrm")?;

        let tables = pool.get_master_table()
            .await
            .wrap(|| "failed to get master table in codegen")?;

        let mut states = Vec::new();

        for table in &tables {
            let field_names: Vec<String> = table.cols.iter()
                .filter(|c| c.name != "id")
                .map(|c| c.name.clone())
                .collect();

            let id_col = table.cols.iter()
                .find(|c| c.name == "id")
                .ok_or_else(|| at!("table '{}' must have an 'id' column", table.name))?;

            let id_type = id_col.kind.rust_type();
            let id_type = if id_col.not_null == 0 {
                // format!("Option<i32>")
                format!("Option<{}>", id_type)
            } else {
                id_type.to_string()
            };

            let base_struct_name = table.name.to_case(Case::Pascal);

            let imports: Vec<Import> = table.cols.iter()
                .filter(|c| c.kind.requires_import().1)
                .inspect(|c| {
                    let (path, _) = c.kind.requires_import();
                    println!("cargo:warning=Your database is using a type which requires another crate: {}", path);
                })
                .map(|c| Import::from_dbtype(c.kind.requires_import()))
                .collect();

            let fk_state: Vec<ForeignKeyState> = table.fks.iter()
                .map(|fk| {
                    let base_name = format!("{}{}", fk.table.to_case(Case::Pascal), fk.ref_table.to_case(Case::Pascal));
                    let table_name_stripped = fk.table.unpluralise();
                    let ref_table_stripped = fk.ref_table.unpluralise();

                    if fk.is_return {
                        ForeignKeyState {
                            join_method: format!("join_{}", fk.table.to_case(Case::Snake)),
                            left_join_method: format!("left_join_{}", fk.table.to_case(Case::Snake)),
                            right_join_method: format!("right_join_{}", fk.table.to_case(Case::Snake)),

                            one_to_many_name: format!("OneToMany{}", base_name),
                            many_to_one_name: format!("ManyToOne{}", base_name),
                            trait_name: format!("Aggregate{}", base_name),
                            row_name: format!("Aggregation{}", base_name),
                            fetch_func_name: format!("fetch_with_{}", table_name_stripped)

                        }
                    } else {
                        ForeignKeyState {
                            join_method: format!("join_{}", fk.ref_table.to_case(Case::Snake)),
                            left_join_method: format!("left_join_{}", fk.ref_table.to_case(Case::Snake)),
                            right_join_method: format!("right_join_{}", fk.ref_table.to_case(Case::Snake)),

                            one_to_many_name: format!("OneToMany{}", base_name),
                            many_to_one_name: format!("ManyToOne{}", base_name),
                            trait_name: format!("Aggregate{}", base_name),
                            row_name: format!("Aggregation{}", base_name),
                            fetch_func_name: format!("fetch_with_{}", ref_table_stripped)
                        }
                    }
                })
                .collect();

            states.push(TableState {
                imports,
                fks: fk_state,
                general: GeneralTableState {
                    select_fields: table.cols.iter()
                        .map(|c| c.name.clone())
                        .collect::<Vec<_>>()
                        .join(", "),
                    field_names: field_names.clone(),
                    field_list: field_names.join(", "),
                    rust_field_names: field_names.iter()
                        .map(|f| f.to_case(Case::Snake))
                        .collect(),
                    placeholders: (0..field_names.len())
                        .map(|i| pool.type_mapper().placeholder(i))
                        .collect::<Vec<_>>()
                        .join(", "),
                    update_fields: field_names
                        .iter()
                        .enumerate()
                        .map(|(i, f)| format!("{} = {}", f, pool.type_mapper().placeholder(i)))
                        .collect::<Vec<_>>()
                        .join(", "),
                    id_type,
                    base_struct_name: base_struct_name.clone(),
                    table_struct_name: format!("{}", base_struct_name),
                    query_struct_name: format!("{}Query", base_struct_name),
                    column_struct_name: format!("{}Column", base_struct_name),
                    builder_struct_name: format!("{}Builder", base_struct_name),
                } 
            });
        }

        let fake_data_gen = PatternBuilder::new();

        let x = Self {
            crate_name: crate_name.into(),
            pool,
            tables,
            states,
            fake_data: fake_data_generator.unwrap_or(fake_data_gen)
        };

        Ok(x)
    }

    /// Run the codegen for all tables in self.tables
    pub fn run_codegen_for_tables(&self) -> LuhTwin<Vec<RustStringView>> {
        let mut table_views = Vec::new();

        for (table, state) in self.tables.iter().zip(self.states.iter()) {
            let mut table_file = RustStringView::new(table.name.to_lowercase());

            table_file.doc(f!("Generated table codegen for \"{}\" - DONT EDIT", table.name));
            table_file.doc_blank();
            table_file.doc(f!("This module contains all generated code for interacting with the \"{}\" table", table.name));
            table_file.doc("It provides type-safe CRUD operations, query builders, generic builders with fake data generators, and relationship handling");
            table_file.doc_blank();
            table_file.doc("## Quick Examples");
            table_file.doc_blank();
            table_file.doc("```ignore");
            table_file.doc(f!("use orm::{}::{{{}, {}, {}}};", table.name, state.general.table_struct_name, state.general.query_struct_name, state.general.builder_struct_name));
            table_file.doc_blank();
            table_file.doc("// basic crud");
            table_file.doc(f!("let row = {}::new().populate_fake_data().build();", state.general.builder_struct_name));
            table_file.doc("row.insert(&pool).await?;");
            table_file.doc_blank();
            table_file.doc("// Type-safe queries");
            table_file.doc(f!("let results = {}::query()", state.general.table_struct_name));
            table_file.doc("    .field_name(\"value\")");
            table_file.doc("    .numeric_field_gt(100)");
            table_file.doc("    .limit(10)");
            table_file.doc("    .fetch_all(&pool)");
            table_file.doc("    .await?;");
            table_file.doc_blank();
            table_file.doc("// relationships with foreign keys");
            table_file.doc(f!("let with_related = {}::query()", state.general.table_struct_name));
            table_file.doc("    .join_related_table()");
            table_file.doc("    .fetch_with_related(&pool)");
            table_file.doc("    .await?");
            table_file.doc("    .one_to_many()?;");
            table_file.doc("```");

            // table_file.line("use serde::Serialize;");
            table_file.line("use sqlx::FromRow;");
            table_file.line("use std::borrow::Cow;");

            table_file.blank();

            table_file.line(f!("use crate::{}::{{ColumnRef, Join, JoinType}};", self.crate_name));

            table_file.blank();

            table_file.doc(f!("# {}", state.general.column_struct_name));
            table_file.doc_blank();
            table_file.doc(f!("This is a generated column struct for {} table", table.name));
            table_file.doc(f!("it has an enum member for every column in your table"));
            table_file.doc(f!("you can this use in combination with [`ColumnRef`]"));
            table_file.doc(f!("inside of [`{}`] to make type safe comparative queries", state.general.query_struct_name));
            table_file.doc_blank();
            table_file.doc(f!("## Provided Methods"));
            table_file.doc_blank();
            table_file.doc(f!("## `pub fn as_str(&self) -> &'static str`"));
            table_file.doc_blank();
            table_file.doc(f!("This returns a mapping of [`{}`] to the column name in your database", state.general.column_struct_name));
            table_file.doc_blank();
            table_file.doc(f!("## `pub fn qualified(&self, table: &str) -> String`"));
            table_file.doc_blank();
            table_file.doc(f!("This returns a mapping of [`{}`] to the column name in your database (you must supply it)", state.general.column_struct_name));
            table_file.doc(f!("as a qualified path e.g X.Y where X is the table and Y is the column name in your database"));
            table_file.doc_blank();
            table_file.doc(f!("## `pub fn of_table(&self) -> String`"));
            table_file.doc_blank();
            table_file.doc(f!("This returns a mapping of [`{}`] to the column name in your database as a qualified path", state.general.column_struct_name));
            table_file.doc(f!("e.g X.Y where X is the table and Y is the column name in your database"));
            table_file.doc(f!("this function has the table name associated with this column baked in at codegen"));
            table_file.doc_blank();
            table_file.doc(f!("## `pub fn of(self) -> ColumnRef`"));
            table_file.doc_blank();
            table_file.doc(f!("This returns a mapping of [`{}`] to the column name in your database", state.general.column_struct_name));
            table_file.doc(f!("this function returns that mapping as a [`ColumnRef`] so you can run"));
            table_file.doc(f!("so you can run different comparative options to build a query"));

            table_file.line("#[derive(Debug, Clone, Copy)]");
            table_file.block(f!("pub enum {} {{", state.general.column_struct_name), |b| {
                for col in &table.cols {
                    let variant_name = col.field_name.to_case(Case::Pascal);
                    b.line(f!("{},", variant_name));
                }
            });

            table_file.blank();
            
            table_file.block(f!("impl {} {{", state.general.column_struct_name), |b| {
                b.doc(f!("This returns a mapping of [`{}`] to the column name in your database", state.general.column_struct_name));
                b.block("pub fn as_str(&self) -> &'static str {", |b| {
                    b.line("match self {");
                    for col in &table.cols {
                        let variant = col.field_name.to_case(Case::Pascal);
                        b.line(f!("    Self::{} => \"{}\",", variant, col.name));
                    }
                    b.line("}");
                });

                b.blank();
                
                b.doc(f!("This returns a mapping of [`{}`] to the column name in your database as a qualified path", state.general.column_struct_name));
                b.doc(f!("e.g X.Y where X is the table and Y is the column name in your database"));
                b.block("pub fn qualified(&self, table: &str) -> String {", |b| {
                    b.line("format!(\"{}.{}\", table, self.as_str())");
                });

                b.blank();
                
                b.doc(f!("This returns a mapping of [`{}`] to the column name in your database as a qualified path", state.general.column_struct_name));
                b.doc(f!("e.g X.Y where X is the table and Y is the column name in your database"));
                b.doc(f!("this function has the table name associated with this column baked in at codegen"));
                b.block("pub fn of_table(&self) -> String {", |b| {
                    b.line(f!("format!(\"{}.{{}}\", self.as_str())", table.name));
                });

                b.blank();

                b.doc(f!("This returns a mapping of [`{}`] to the column name in your database", state.general.column_struct_name));
                b.doc(f!("this function returns that mapping as a [`ColumnRef`] so you can run"));
                b.doc(f!("so you can run different comparative options to build a query"));
                b.block("pub fn of(self) -> ColumnRef {", |b| {
                    b.line("ColumnRef {");
                    b.line(f!("    table: \"{}\".to_string(),", table.name));
                    b.line("    column: self.as_str().to_string(),");
                    b.line("}");
                });

            });

            table_file.blank();

            table_file.doc(f!("# {}", state.general.table_struct_name));
            table_file.doc_blank();
            table_file.doc(f!("This is a generated struct depicting your {} table", table.name));
            table_file.doc(f!("this struct has methods for insert, update, delete, get_by_id and get_all"));
            table_file.doc(f!("more importantly though it contains a query method which returns [`{}`]", state.general.query_struct_name));
            table_file.doc(f!("you can use this to interface to your table with safe types"));
            table_file.doc_blank();
            table_file.doc("## Examples");
            table_file.doc_blank();
            table_file.doc("```ignore");
            table_file.doc("let row = User {");
            table_file.doc("    id: None");
            table_file.doc("    name: \"bine\".to_string()");
            table_file.doc("    password: some_hash.to_string()");
            table_file.doc("    date_added: SystemTime::now()");
            table_file.doc("        .duration_since(UNIX_EPOCH)");
            table_file.doc("        .unwrap()");
            table_file.doc("        .as_secs() as i32");
            table_file.doc("}");
            table_file.doc_blank();
            table_file.doc("row.insert(&pool).await?;");
            table_file.doc("```");
            table_file.doc_blank();
            table_file.doc("```ignore");
            table_file.doc("let users = Users::get_all(&pool)");
            table_file.doc("    .await");
            table_file.doc("    .wrap(|| \"failed to get all users out of the pool (propagate_db)\")?;");
            table_file.doc_blank();
            table_file.doc("for user in users {");
            table_file.doc("    // some logic");
            table_file.doc("}");
            table_file.doc("```");
            table_file.doc_blank();
            table_file.doc(f!("*see [`{}`] for an example on the query builder*", state.general.query_struct_name));
            table_file.doc_blank();
            table_file.doc("## Provided Methods");
            table_file.doc_blank();
            table_file.doc("## `pub async fn insert(&self, pool: &sqlx::{}) -> Result<(), sqlx::Error>`");
            table_file.doc_blank();
            table_file.doc(f!("Inserts self into {} table through binds on a query and then executing that query on the pool supplied", table.name));
            table_file.doc_blank();
            table_file.doc("## `pub async fn update(&self, pool: &sqlx::{}) -> Result<(), sqlx::Error>`");
            table_file.doc_blank();
            table_file.doc(f!("Updates self in {} table through binds on a query and then executing that query on the pool supplied", table.name));
            table_file.doc("this updates according to id to any entry with a matching id (so it assumes unique ids but not mandatory)");
            table_file.doc("it will fail if no rows where found");
            table_file.doc_blank();
            table_file.doc("## `pub async fn delete(id: {}, pool: &sqlx::{}) -> Result<(), sqlx::Error>`");
            table_file.doc_blank();
            table_file.doc(f!("Deletes self from {} table according to id on the pool supplied", table.name));
            table_file.doc("since this deletes according to id to any entry with a matching id (so it assumes unique ids but not mandatory)");
            table_file.doc_blank();
            table_file.doc("## `pub async fn get_by_id(id: {}, pool: &sqlx::{}) -> Result<Option<Self>, sqlx::Error>`");
            table_file.doc_blank();
            table_file.doc(f!("Selects all from {} table matching to the id supplied on the pool which is also supplied", table.name));
            table_file.doc_blank();
            table_file.doc("## `pub async fn get_all(pool: &sqlx::{}) -> Result<Vec<Self>, sqlx::Error>`");
            table_file.doc_blank();
            table_file.doc(f!("Selects all entries from {} table", table.name));
            table_file.doc_blank();
            table_file.doc("## `pub fn query() -> {}`");
            table_file.doc_blank();
            table_file.doc("This returns the corresponding query struct associated with this table");

            table_file.line("#[derive(Debug, Clone, serde::Serialize, FromRow)]");

            table_file.block(f!("pub struct {} {{", state.general.table_struct_name), |b| {
                for col in &table.cols {
                    let rust_type = col.kind.rust_type();
                    let field_name = col.field_name.to_case(Case::Snake);

                    b.line(f!("#[sqlx(rename = \"{}\")]", col.name));

                    if col.name == "id" {
                        b.line(f!("pub id: {},", state.general.id_type));
                    } else {
                        if col.not_null == 0 {
                            b.line(f!("pub {}: Option<{}>,", field_name, rust_type));
                        } else {
                            b.line(f!("pub {}: {},", field_name, rust_type));
                        }
                    }

                    b.blank();
                }
            });

            table_file.blank();

            table_file.block(f!("impl {} {{", state.general.table_struct_name), |b| {
                b.doc("The name of this table in your database");
                b.line(f!("pub const NAME: &'static str = \"{}\";", table.name));

                b.blank();

                // b.line("// column mappers");

                for col in &table.cols {
                    let const_name = col.name.to_case(Case::UpperSnake);
                    let variant = col.field_name.to_case(Case::Pascal);
                    b.doc(f!("Mapper for \"{}\" column it maps to the corresponding [`{}`] enum member for this column", col.name, state.general.column_struct_name));
                    b.line(f!("pub const {}: {} = {}::{};", const_name, state.general.column_struct_name, state.general.column_struct_name, variant));
                    b.blank();
                }

                b.doc(f!("Inserts self into {} table through binds on a query and then executing that query on the pool supplied", table.name));
                b.block(f!("pub async fn insert(&self, pool: &sqlx::{}) -> Result<(), sqlx::Error> {{", self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!(
                        "let mut q = sqlx::query(\"INSERT INTO {} ({}) VALUES ({})\");",
                        table.name, state.general.field_list, state.general.placeholders
                    ));

                    b.blank();

                    for f in &state.general.rust_field_names {
                        b.line(f!("q = q.bind(&self.{});", f));
                    }

                    b.blank();

                    b.line("let _ = q.execute(pool).await?;");
                    b.line("Ok(())");
                });

                b.blank();

                b.doc(f!("Updates self in {} table through binds on a query and then executing that query on the pool supplied", table.name));
                b.doc("this updates according to id to any entry with a matching id (so it assumes unique ids but not mandatory)");
                b.doc("it will fail if no rows where found");
                b.block(f!("pub async fn update(&self, pool: &sqlx::{}) -> Result<u64, sqlx::Error> {{", self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!(
                        "let mut q = sqlx::query(\"UPDATE {} SET {} WHERE id = {}\");",
                        table.name, state.general.update_fields, self.pool.type_mapper().placeholder(state.general.field_names.len())
                    ));

                    for f in &state.general.rust_field_names {
                        b.line(f!("q = q.bind(&self.{});", f));
                    }

                    b.line("q = q.bind(&self.id);");

                    b.blank();

                    b.line("let result = q.execute(pool).await?;");

                    // b.block("if result.rows_affected() == 0 {", |b| {
                    //     b.line("return Err(sqlx::Error::RowNotFound);");
                    // });

                    b.line("Ok(result.rows_affected())");
                });

                b.blank();
               
                b.doc(f!("Deletes self from {} table according to id on the pool supplied", table.name));
                b.doc("since this deletes according to id to any entry with a matching id (so it assumes unique ids but not mandatory)");
                b.block(f!("pub async fn delete(id: {}, pool: &sqlx::{}) -> Result<(), sqlx::Error> {{", state.general.id_type, self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!("sqlx::query(\"DELETE FROM {} WHERE id = {}\")", table.name, self.pool.type_mapper().placeholder(0)));
                    b.line("    .bind(id)");
                    b.line("    .execute(pool)");
                    b.line("    .await?;");
                    b.line("Ok(())");
                });

                b.blank();
                
                b.doc(f!("Selects all from {} table matching to the id supplied on the pool which is also supplied", table.name));
                b.block(f!("pub async fn get_by_id(id: {}, pool: &sqlx::{}) -> Result<Option<Self>, sqlx::Error> {{", state.general.id_type, self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!(
                        "let row = sqlx::query_as::<_, Self>(\"SELECT {} FROM {} WHERE id = {}\")",
                        state.general.select_fields, table.name, self.pool.type_mapper().placeholder(0)
                    ));
                    b.line("    .bind(id)");
                    b.line("    .fetch_optional(pool)");
                    b.line("    .await?;");
                    b.line("Ok(row)");
                });

                b.blank();

                b.doc(f!("Selects all entries from {} table", table.name));
                b.block(f!("pub async fn get_all(pool: &sqlx::{}) -> Result<Vec<Self>, sqlx::Error> {{", self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!("let rows = sqlx::query_as::<_, Self>(\"SELECT {} FROM {}\")", state.general.select_fields, table.name));
                    b.line("    .fetch_all(pool)");
                    b.line("    .await?;");
                    b.line("Ok(rows)");
                });

                b.blank();

                b.doc("This returns the corresponding query struct associated with this table");
                b.block(f!("pub fn query<'a>() -> {}<'a> {{", state.general.query_struct_name), |b| {
                    b.line(f!("{}::default()", state.general.query_struct_name));
                });
            });
            
            table_file.blank();

            table_file.doc(f!("# {}", state.general.query_struct_name));
            table_file.doc_blank();
            table_file.doc(f!("This is generated struct design to query your {} table", table.name));
            table_file.doc("it contains methods to set every column of your table ");
            table_file.doc("(the methods are named after the names of the columns)");
            table_file.doc("it contains greater than, less than, greater or equal too,");
            table_file.doc("less or equal too  operations for numeric columns.");
            table_file.doc("It has join capabilities and custom where clause capabilities");
            table_file.doc("along with limit and offset capabilities.");
            table_file.doc(f!("You can use [`{}::fetch_all()`] and [`{}::fetch_one()`] to fetch your query", state.general.query_struct_name, state.general.query_struct_name));
            table_file.doc_blank();
            table_file.doc("## Examples");
            table_file.doc_blank();
            table_file.doc("todo soon");
            table_file.doc_blank();
            table_file.doc("## Provided Methods");
            table_file.doc_blank();
            table_file.doc("## `pub fn join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self`");
            table_file.doc_blank();
            table_file.doc("Creates an inner join on table on left [`ColumnRef`] = right [`ColumnRef`]");
            table_file.doc_blank();
            table_file.doc("## `pub fn left_join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self`");
            table_file.doc_blank();
            table_file.doc("Creates a left join on table on left [`ColumnRef`] = right [`ColumnRef`]");
            table_file.doc_blank();
            table_file.doc("## `pub fn right_join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self`");
            table_file.doc_blank();
            table_file.doc("Creates a right join on table on left [`ColumnRef`] = right [`ColumnRef`]");
            table_file.doc_blank();
            table_file.doc("## `pub fn where(mut self, condition: String) -> Self`");
            table_file.doc_blank();
            table_file.doc("Allows a where clause to be added it could be a raw string or generated with a [`ColumnRef`]");
            table_file.doc_blank();
            table_file.doc("## `pub fn limit(mut self, value: i32) -> Self`");
            table_file.doc_blank();
            table_file.doc("Sets a limit of how many results to fetch when query is executed");
            table_file.doc_blank();
            table_file.doc("## `pub fn offset(mut self, value: i32) -> Self`");
            table_file.doc_blank();
            table_file.doc("Sets an offset for the results that are returned when the query is executed");
            table_file.doc("offset meaning here if there was 10 results returned and the offset was 2 then we do not return the first 2 results");
            table_file.doc_blank();
            table_file.doc(f!("## `pub async fn execute_query<T: for <'r> sqlx::FromRow<'r, sqlx::{}> + Send + Unpin>(self, mut sql: String, pool: &sqlx::{}) -> Result<Vec<T>, sqlx::Error>`", self.pool.type_mapper().sqlx_row_type(), self.pool.type_mapper().sqlx_type()));
            table_file.doc("This will run the query given as a parameter");
            table_file.doc("this function is dangerous...");
            table_file.doc("on a first note it expects you pass in a query given \"SELECT {} FROM {}\"");
            table_file.doc("given with no semicolon on the end");
            table_file.doc("this will return the raw rows the only reason this is public is so you can still run your raw query");
            table_file.doc("if something is limiting you");
            table_file.doc_blank();
            table_file.doc(f!("## `pub async fn fetch_all(self, pool: &sqlx::{}) -> Result<Vec<Self>, sqlx::Error>`", self.pool.type_mapper().sqlx_type()));
            table_file.doc_blank();
            table_file.doc("Runs the query (self)");
            table_file.doc_blank();
            table_file.doc(f!("## `pub async fn fetch_one(self, pool: &sqlx::{}) -> Result<Option<Self>, sqlx::Error>`", self.pool.type_mapper().sqlx_type()));
            table_file.doc_blank();
            table_file.doc("Runs the query (self) with a limit of one");
            table_file.doc_blank();
            table_file.doc("## Generic Methods");
            table_file.doc_blank();
            table_file.doc("*there will be one of these for every field/foreign key in your table we just show docs for one though*");
            table_file.doc_blank();

            let fks_and_state = table.fks.iter()
                .zip(state.fks.iter())
                .collect::<Vec<(&ForeignKey, &ForeignKeyState)>>();

            if let Some((fk, fk_state)) = fks_and_state.first() {
                table_file.doc(f!("## `pub async fn {}(self, pool: &sqlx::{}) -> Result<Vec<{}>, sqlx::Error>`", fk_state.fetch_func_name, self.pool.type_mapper().sqlx_type(), fk_state.row_name));
                table_file.doc_blank();
                table_file.doc(f!("Runs the query(self) respecting the joins between table \"{}\" and \"{}\" that are expected to occur", fk.table, fk.ref_table));
                table_file.doc_blank();
                
                table_file.doc(f!("## `pub fn {}(mut self) -> Self`", fk_state.join_method));
                table_file.doc_blank();
                table_file.doc(f!("Creates an inner join to {} on {}.{} = {}.{}", fk.ref_table, fk.table, fk.column, fk.ref_table, fk.ref_column));
                table_file.doc_blank();
                
                table_file.doc(f!("pub fn {}(mut self) -> Self", fk_state.left_join_method));
                table_file.doc_blank();
                table_file.doc(f!("Creates a left join to {} on {}.{} = {}.{}", fk.ref_table, fk.table, fk.column, fk.ref_table, fk.ref_column));
                table_file.doc_blank();
                
                table_file.doc(f!("pub fn {}(mut self) -> Self", fk_state.right_join_method));
                table_file.doc_blank();
                table_file.doc(f!("Creates a right join to {} on {}.{} = {}.{}", fk.ref_table, fk.table, fk.column, fk.ref_table, fk.ref_column));
                
                table_file.doc_blank();
            }

            let col = table.cols.first()
                .ok_or_else(|| {
                    "no cols in table.cols for some reason (Query struct docs)"
                })?;

            let (rust_type, is_cow) = col.kind.cow_rust_type();
            let field_name = col.field_name.to_case(Case::Snake);

            if is_cow {
                table_file.doc(f!("## `pub fn {}<'a>(mut self, value: impl Into<{}>) -> Self`", field_name, rust_type));
            } else {
                table_file.doc(f!("## `pub fn {}(mut self, value: {}) -> Self`", field_name, rust_type));
            }

            table_file.doc_blank();
            table_file.doc(f!("Sets the value of {} inside the query builder", field_name));
            table_file.doc_blank();
            
            if col.is_numeric() {
                let base_type = col.kind.rust_type();
                
                table_file.doc(f!("## `pub fn {}_gt(mut self, value: {}) -> Self`", field_name, base_type));
                table_file.doc_blank();
                table_file.doc(f!("Sets the value of greater than option of numeric {} column", field_name));
                table_file.doc_blank();

                table_file.doc(f!("## `pub fn {}_gte(mut self, value: {}) -> Self`", field_name, base_type));
                table_file.doc_blank();
                table_file.doc(f!("Sets the value of greater than or equal too option of numeric {} column", field_name));
                table_file.doc_blank();

                table_file.doc(f!("## `pub fn {}_lt(mut self, value: {}) -> Self`", field_name, base_type));
                table_file.doc_blank();
                table_file.doc(f!("Sets the value of less than option of numeric {} column", field_name));
                table_file.doc_blank();

                table_file.doc(f!("pub fn {}_lte(mut self, value: {}) -> Self", field_name, base_type));
                table_file.doc_blank();
                table_file.doc(f!("Sets the value of less than or equal too option of numeric {} column", field_name));
                table_file.doc_blank();

                table_file.doc(f!("pub fn {}_between(mut self, start: {}, end: {}) -> Self", field_name, base_type, base_type));
                table_file.doc_blank();
                table_file.doc(f!("Sets the value of between option of numeric {} column", field_name));
                table_file.doc(f!("this sets both greater and less than or equal too options"));
                table_file.doc_blank();
            }

            table_file.line("#[derive(Debug, Clone, Default)]");

            table_file.block(f!("pub struct {}<'a> {{", state.general.query_struct_name), |b| {
                for col in &table.cols {
                    let rust_type = col.kind.cow_rust_type();

                    let name = col.name.to_case(Case::Snake);

                    b.line(f!("pub {}: Option<{}>,", name, rust_type.0));

                    if col.is_numeric() {
                        let base_type = col.kind.rust_type();

                        b.line(f!("pub {}_gt: Option<{}>,", name, base_type));
                        b.line(f!("pub {}_gte: Option<{}>,", name, base_type));
                        b.line(f!("pub {}_lt: Option<{}>,", name, base_type));
                        b.line(f!("pub {}_lte: Option<{}>,", name, base_type));
                    }
                }

                b.line("pub joins: Vec<Join>,");
                b.line("pub where_clauses: Vec<String>,");
                b.line("pub limit: Option<i32>,");
                b.line("pub offset: Option<i32>,");
            });

            table_file.blank();

            table_file.block(f!("impl<'a> {}<'a> {{", state.general.query_struct_name), |b| {
                for col in &table.cols {
                    let (rust_type, is_cow) = col.kind.cow_rust_type();

                    let field_name = col.field_name.to_case(Case::Snake);

                    b.doc(f!("Sets the value of {} inside the query builder", field_name));
                    if is_cow {
                        b.block(f!("pub fn {}(mut self, value: impl Into<{}>) -> Self {{", field_name, rust_type), |b| {
                            b.line(f!("self.{} = Some(value.into());", field_name));
                            b.line("self");
                        });
                    } else {
                        b.block(f!("pub fn {}(mut self, value: {}) -> Self {{", field_name, rust_type), |b| {
                            b.line(f!("self.{} = Some(value);", field_name));
                            b.line("self");
                        });
                    }

                    b.blank();
                    
                    if col.is_numeric() {
                        let base_type = col.kind.rust_type();
                        
                        b.doc(f!("Sets the value of greater than option of numeric {} column", field_name));
                        b.block(f!("pub fn {}_gt(mut self, value: {}) -> Self {{", field_name, base_type), |b| {
                            b.line(f!("self.{}_gt = Some(value);", field_name));
                            b.line("self");
                        });

                        b.blank();

                        b.doc(f!("Sets the value of greater than or equal too option of numeric {} column", field_name));
                        b.block(f!("pub fn {}_gte(mut self, value: {}) -> Self {{", field_name, base_type), |b| {
                            b.line(f!("self.{}_gte = Some(value);", field_name));
                            b.line("self");
                        });

                        b.blank();

                        b.doc(f!("Sets the value of less than option of numeric {} column", field_name));
                        b.block(f!("pub fn {}_lt(mut self, value: {}) -> Self {{", field_name, base_type), |b| {
                            b.line(f!("self.{}_lt = Some(value);", field_name));
                            b.line("self");
                        });

                        b.blank();

                        b.doc(f!("Sets the value of less than or equal too option of numeric {} column", field_name));
                        b.block(f!("pub fn {}_lte(mut self, value: {}) -> Self {{", field_name, base_type), |b| {
                            b.line(f!("self.{}_lte = Some(value);", field_name));
                            b.line("self");
                        });
                        
                        b.blank();
                        
                        b.doc(f!("Sets the value of between option of numeric {} column", field_name));
                        b.doc(f!("this sets both greater and less than or equal too options"));
                        b.block(f!("pub fn {}_between(mut self, start: {}, end: {}) -> Self {{", field_name, base_type, base_type), |b| {
                            b.line(f!("self.{}_gte = Some(start);", field_name));
                            b.line(f!("self.{}_lte = Some(end);", field_name));
                            b.line("self");
                        });

                        b.blank();
                    }
                }

                for (fk, fk_state) in table.fks.iter().zip(state.fks.iter()) {
                    let fk_ref_column = if fk.is_return {
                        fk.column.to_case(Case::Pascal)
                    } else {
                        fk.ref_column.to_case(Case::Pascal)
                    };

                    let ref_table_name = if fk.is_return {
                        fk.table.clone()
                    } else {
                        fk.ref_table.clone()
                    };

                    let struct_name = if fk.is_return {
                        fk.table.to_case(Case::Pascal)
                    } else {
                        fk.ref_table.to_case(Case::Pascal)
                    };

                    let column_name = if fk.is_return {
                        fk.ref_column.to_case(Case::Pascal)
                    } else {
                        fk.column.to_case(Case::Pascal)
                    };

                    b.doc(f!("Creates an inner join to {} on {}.{} = {}.{}", fk.table, fk.table, fk.column, fk.ref_table, fk.ref_column));
                    b.block(f!("pub fn {}(mut self) -> Self {{", fk_state.join_method), |b| {
                        b.line(f!("self.joins.push(Join {{"));
                        b.line(f!("    join_type: JoinType::Inner,"));
                        b.line(f!("    table: \"{}\".to_string(),", ref_table_name));
                        b.line(f!("    on_condition: format!(\"{}.{{}} = {}.{{}}\", ", table.name, ref_table_name));
                        b.line(f!("        {}::{}.as_str(), ", state.general.column_struct_name, column_name));
                        b.line(f!("        crate::{}::{}::{}Column::{}.as_str()),", self.crate_name, ref_table_name, struct_name, fk_ref_column));
                        b.line(f!("}});"));
                        b.line(f!("self"));
                    });

                    b.blank();
                    
                    b.doc(f!("Creates an left join to {} on {}.{} = {}.{}", fk.table, fk.table, fk.column, fk.ref_table, fk.ref_column));
                    b.block(f!("pub fn {}(mut self) -> Self {{", fk_state.left_join_method), |b| {
                        b.line(f!("self.joins.push(Join {{"));
                        b.line(f!("    join_type: JoinType::Left,"));
                        b.line(f!("    table: \"{}\".to_string(),", ref_table_name));
                        b.line(f!("    on_condition: format!(\"{}.{{}} = {}.{{}}\", ", table.name, ref_table_name));
                        b.line(f!("        {}Column::{}.as_str(), ", state.general.base_struct_name, column_name));
                        b.line(f!("        crate::{}::{}::{}Column::{}.as_str()),", self.crate_name, ref_table_name, struct_name, fk_ref_column));
                        b.line(f!("}});"));
                        b.line(f!("self"));
                    });

                    b.blank();
                    
                    b.doc(f!("Creates an right join to {} on {}.{} = {}.{}", fk.table, fk.table, fk.column, fk.ref_table, fk.ref_column));
                    b.block(f!("pub fn {}(mut self) -> Self {{", fk_state.right_join_method), |b| {
                        b.line(f!("self.joins.push(Join {{"));
                        b.line(f!("    join_type: JoinType::Right,"));
                        b.line(f!("    table: \"{}\".to_string(),", ref_table_name));
                        b.line(f!("    on_condition: format!(\"{}.{{}} = {}.{{}}\", ", table.name, ref_table_name));
                        b.line(f!("        {}Column::{}.as_str(), ", state.general.base_struct_name, column_name));
                        b.line(f!("        crate::{}::{}::{}Column::{}.as_str()),", self.crate_name, ref_table_name, struct_name, fk_ref_column));
                        b.line(f!("}});"));
                        b.line(f!("self"));
                    });

                    b.blank();

                    let table_name_stripped = fk.table.unpluralise();
                    
                    let row_name = format!("crate::{}::{}::{}", self.crate_name, fk.table, fk_state.row_name);

                    b.doc(f!("Runs the query(self) respecting the joins between table \"{}\" and \"{}\" that are expected to occur", fk.table, fk.ref_table));
                    b.block(f!("pub async fn {}(self, pool: &sqlx::{}) -> Result<Vec<{}>, sqlx::Error> {{", fk_state.fetch_func_name, self.pool.type_mapper().sqlx_type(), row_name), |b| {
                        let mut fields: Vec<String> = Vec::new();
                        
                        let mut used_names: Vec<String> = Vec::new();
                        
                        let table_name_backwards = if fk.is_return {
                            fk.ref_table.clone()
                        } else {
                            fk.table.clone()
                        };
                        
                        let ref_table_name_backwards = if fk.is_return {
                            fk.table.clone()
                        } else {
                            fk.ref_table.clone()
                        };
                        
                        table.cols.iter()
                            .for_each(|c| {
                                fields.push(format!("{}.{} AS {}", table_name_backwards, c.name, c.name));
                                used_names.push(c.name.clone());
                            });
                        
                        self.tables.iter()
                            .filter(|t| t.name == ref_table_name_backwards)
                            .for_each(|c| {
                                c.cols.iter()
                                    .for_each(|c| {
                                        if used_names.contains(&c.name) {
                                            fields.push(format!("{}.{} AS {}_{}", ref_table_name_backwards, c.name, table_name_stripped, c.name));
                                        } else {
                                            fields.push(format!("{}.{} AS {}", ref_table_name_backwards, c.name, c.name));
                                        }
                                    })
                            });
                        
                        b.line(f!("let sql = String::from(\"SELECT {} FROM {}\");", fields.join(", "), table.name));
                        
                        b.blank();
                        
                        b.line(f!("self.execute_query::<{}>(sql, pool).await", row_name));
                    });
                }

                b.blank();

                b.doc("Creates an inner join on table on left [`ColumnRef`] = right [`ColumnRef`]");
                b.block("pub fn join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self {", |b| {
                    b.line("self.joins.push(Join {");
                    b.line("    join_type: JoinType::Inner,");
                    b.line("    table: table.into(),");
                    b.line("    on_condition: left.eq(&right),");
                    b.line("});");
                    b.line("self");
                });

                b.blank();
                
                b.doc("Creates a left join on table on left [`ColumnRef`] = right [`ColumnRef`]");
                b.block("pub fn left_join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self {", |b| {
                    b.line("self.joins.push(Join {");
                    b.line("    join_type: JoinType::Left,");
                    b.line("    table: table.into(),");
                    b.line("    on_condition: left.eq(&right),");
                    b.line("});");
                    b.line("self");
                });

                b.blank();
                
                b.doc("Creates a right join on table on left [`ColumnRef`] = right [`ColumnRef`]");
                b.block("pub fn right_join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self {", |b| {
                    b.line("self.joins.push(Join {");
                    b.line("    join_type: JoinType::Right,");
                    b.line("    table: table.into(),");
                    b.line("    on_condition: left.eq(&right),");
                    b.line("});");
                    b.line("self");
                });

                b.blank();

                b.doc("Allows a where clause to be added it could be a raw string or generated with a [`ColumnRef`]");
                b.block("pub fn where_clause(mut self, condition: String) -> Self {", |b| {
                    b.line("self.where_clauses.push(condition);");
                    b.line("self");
                });

                b.blank();

                b.doc("Sets a limit of how many results to fetch when query is executed");
                b.block("pub fn limit(mut self, value: i32) -> Self {", |b| {
                    b.line("self.limit = Some(value);");
                    b.line("self");
                });

                b.blank();

                b.doc("Sets an offset for the results that are returned when the query is executed");
                b.doc("offset meaning here if there was 10 results returned and the offset was 2 then we do not return the first 2 results");
                b.block("pub fn offset(mut self, value: i32) -> Self {", |b| {
                    b.line("self.offset = Some(value);");
                    b.line("self");
                });

                b.blank();

                b.doc("Runs the query(self)");
                b.block(f!("pub async fn fetch_all(self, pool: &sqlx::{}) -> Result<Vec<{}>, sqlx::Error> {{", self.pool.type_mapper().sqlx_type(), state.general.base_struct_name), |b| {
                    let mut fields: Vec<String> = Vec::new();

                    table.cols.iter()
                        .for_each(|c| {
                            fields.push(format!("\\\"{}\\\"", c.name));
                        });

                    b.line(f!("let sql = String::from(\"SELECT {} FROM {}\");", fields.join(", "), table.name));

                    b.blank();

                    b.line(f!("self.execute_query::<{}>(sql, pool).await", state.general.base_struct_name));
                });

                b.blank();

                b.doc("Runs the query (self) with a limit of one");
                b.block(f!("pub async fn fetch_one(self, pool: &sqlx::{}) -> Result<Option<{}>, sqlx::Error> {{", self.pool.type_mapper().sqlx_type(), state.general.base_struct_name), |b| {
                    b.line("let mut results = self.limit(1).fetch_all(pool).await?;");
                    b.line("Ok(results.pop())");
                });

                b.blank();

                b.doc("This will run the query given as a parameter");
                b.doc("this function is dangerous...");
                b.doc("on a first note it expects you pass in a query given \"SELECT {} FROM {}\"");
                b.doc("given with no semicolon on the end");
                b.doc("this will return the raw rows the only reason this is public is so you can still run your raw query");
                b.doc("if something is limiting you");
                b.block(f!("pub async fn execute_query<T: for <'r> sqlx::FromRow<'r, sqlx::{}> + Send + Unpin>(self, mut sql: String, pool: &sqlx::{}) -> Result<Vec<T>, sqlx::Error> {{", self.pool.type_mapper().sqlx_row_type(), self.pool.type_mapper().sqlx_type()), |b| {
                    b.line("let mut conditions = Vec::new();");

                    if self.pool.type_mapper().uses_numbered_placeholders() {
                        b.line("let mut bind_idx = 0;");
                    }

                    // build condition checks
                    for col in &table.cols {
                        let field_name = col.field_name.to_case(Case::Snake);

                        if self.pool.type_mapper().uses_numbered_placeholders() {
                            b.block(f!("if self.{}.is_some() {{", field_name), |b| {
                                b.line("bind_idx += 1;");
                                b.line(f!("conditions.push(format!(\"{}.{} = ${{}}\", bind_idx));", table.name, col.name));
                            });

                            b.blank();

                            if col.is_numeric() {
                                b.block(f!("if self.{}_gt.is_some() {{", field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} > ${{}}\", bind_idx));", col.name));
                                });

                                b.blank();

                                b.block(f!("if self.{}_gte.is_some() {{", field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} >= ${{}}\", bind_idx));", col.name));
                                });

                                b.blank();

                                b.block(f!("if self.{}_lt.is_some() {{", field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} < ${{}}\", bind_idx));", col.name));
                                });

                                b.blank();
                                
                                b.block(f!("if self.{}_lte.is_some() {{", field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} <= ${{}}\", bind_idx));", col.name));
                                });

                                b.blank();
                            }
                        } else {
                             b.block(f!("if self.{}.is_some() {{", field_name), |b| {
                                b.line(f!("conditions.push(\"{}.{} = ?\".to_string());", table.name, col.name));
                            });

                            b.blank();

                            if col.is_numeric() {
                                b.block(f!("if self.{}_gt.is_some() {{", field_name), |b| {
                                    b.line(f!("conditions.push(\"{} > ?\".to_string());", col.name));
                                });

                                b.blank();

                                b.block(f!("if self.{}_gte.is_some() {{", field_name), |b| {
                                    b.line(f!("conditions.push(\"{} >= ?\".to_string());", col.name));
                                });

                                b.blank();

                                b.block(f!("if self.{}_lt.is_some() {{", field_name), |b| {
                                    b.line(f!("conditions.push(\"{} < ?\".to_string());", col.name));
                                });

                                b.blank();

                                b.block(f!("if self.{}_lte.is_some() {{", field_name), |b| {
                                    b.line(f!("conditions.push(\"{} <= ?\".to_string());", col.name));
                                });

                                b.blank();
                            }
                        }
                    }

                    b.block("for clause in &self.where_clauses {", |b| {
                        b.line("conditions.push(clause.clone());");
                    });

                    b.blank();

                    b.block("if !conditions.is_empty() {", |b| {
                        b.line("sql.push_str(\" WHERE \");");
                        b.line("sql.push_str(&conditions.join(\" AND \"));");
                    });

                    b.blank();

                    b.block("for join in &self.joins {", |b| {
                        b.line("let join_type_str = match join.join_type {");
                        b.line("    JoinType::Inner => \"INNER JOIN\",");
                        b.line("    JoinType::Left => \"LEFT JOIN\",");
                        b.line("    JoinType::Right => \"RIGHT JOIN\",");
                        b.line("};");
                        b.line("sql.push_str(&format!(\" {} {} ON {}\", join_type_str, join.table, join.on_condition));");
                    });

                    b.blank();

                    b.block("if let Some(lim) = self.limit {", |b| {
                        b.line("sql.push_str(&format!(\" LIMIT {}\", lim));");
                    });

                    b.blank();
                    
                    b.block("if let Some(off) = self.offset {", |b| {
                        b.line("sql.push_str(&format!(\" OFFSET {}\", off));");
                    });

                    b.blank();
                    
                    b.line("let mut query = sqlx::query_as(&sql);");

                    b.blank();

                    for col in &table.cols {
                        b.block(f!("if let Some(ref val) = self.{} {{", col.field_name), |b| {
                            let x: Cow<'static, str> = Cow::Borrowed("hello world");
                            x.as_ref();
                            if col.is_string_like() {
                                if col.kind == DbType::Blob {
                                    b.line("query = query.bind(&**val as &[u8]);");
                                } else {
                                    b.line("query = query.bind(&**val);");
                                }
                            } else {
                                b.line("query = query.bind(val);");
                            }
                        });

                        b.blank();

                        if col.is_numeric() {
                            b.block(f!("if let Some(val) = self.{}_gt {{", col.field_name), |b| {
                                b.line("query = query.bind(val);");
                            });

                            b.blank();

                            b.block(f!("if let Some(val) = self.{}_gte {{", col.field_name), |b| {
                                b.line("query = query.bind(val);");
                            });

                            b.blank();

                            b.block(f!("if let Some(val) = self.{}_lt {{", col.field_name), |b| {
                                b.line("query = query.bind(val);");
                            });

                            b.blank();

                            b.block(f!("if let Some(val) = self.{}_lte {{", col.field_name), |b| {
                                b.line("query = query.bind(val);");
                            });

                            b.blank();

                        }
                    }
                    
                    b.line("query.fetch_all(pool).await");
                });
            });

            for (fk, fk_state) in table.fks.iter().zip(state.fks.iter()) {
                if !fk.is_return {
                    table_file.blank();

                    // this code doesn't matter if its a return key or not
                    // just dont want it to generate twice for also the ref_table

                    let table_name = fk.table.to_case(Case::Pascal);
                    let ref_table_name = fk.ref_table.to_case(Case::Pascal);

                    let table_name_stripped = fk.table.unpluralise();
                    let ref_table_name_stripped = fk.ref_table.unpluralise();

                    table_file.doc(f!("{}", fk_state.one_to_many_name));
                    table_file.doc_blank();
                    table_file.doc(f!("This is a generated struct for \"{}\" and \"{}\" tables", fk.table, fk.ref_table));
                    table_file.doc("this struct expresses the foreign key relationship between the two");
                    table_file.doc(f!("as one to many, i.e one {} to many {}", table_name_stripped, ref_table_name_stripped.pluralise()));
                    table_file.doc(f!("this struct is returned from [`{}`] trait", fk_state.trait_name));
                    table_file.line("#[derive(Debug, Clone)]");
                    table_file.block(f!("pub struct {} {{", fk_state.one_to_many_name), |b| {
                        b.line(f!("pub {}: crate::{}::{}::{},", ref_table_name_stripped, self.crate_name, fk.ref_table, ref_table_name));

                        b.line(f!("pub {}: Vec<crate::{}::{}::{}>,", table_name_stripped.pluralise(), self.crate_name, fk.table, table_name));
                    });

                    table_file.blank();

                    table_file.doc(f!("{}", fk_state.many_to_one_name));
                    table_file.doc_blank();
                    table_file.doc(f!("This is a generated struct for \"{}\" and \"{}\" tables", fk.table, fk.ref_table));
                    table_file.doc("this struct expresses the foreign key relationship between the two");
                    table_file.doc(f!("as many to one, i.e one {} to many {}", ref_table_name_stripped, table_name_stripped.pluralise()));
                    table_file.doc(f!("this struct is returned from [`{}`] trait", fk_state.trait_name));
                    table_file.line("#[derive(Debug, Clone)]");
                    table_file.block(f!("pub struct {} {{", fk_state.many_to_one_name), |b| {
                        b.line(f!("pub {}: Vec<crate::{}::{}::{}>,", ref_table_name_stripped.pluralise(), self.crate_name, fk.ref_table, ref_table_name));

                        b.line(f!("pub {}: crate::{}::{}::{},", table_name_stripped, self.crate_name, fk.table, table_name));
                    });

                    table_file.blank();

                    table_file.doc(f!("{}", fk_state.row_name));
                    table_file.doc_blank();
                    table_file.doc(f!("This is a generated struct for \"{}\" and \"{}\" tables", fk.table, fk.ref_table));
                    table_file.doc(f!("any query you run with the {} function will return this type", fk_state.fetch_func_name));
                    table_file.doc("this is the base aggregate type it tells you this is a");
                    table_file.doc("one to one (if all returned data is the same) or many to many (all returned data is different)");
                    table_file.doc(f!("using [`{}`] trait you can aggregate this struct into [`{}`] or [`{}`] forms", fk_state.trait_name, fk_state.one_to_many_name, fk_state.many_to_one_name));
                    table_file.line("#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]");
                    table_file.block(f!("pub struct {} {{", fk_state.row_name), |b| {
                        b.line(f!("#[sqlx(flatten)]"));
                        b.line(f!("pub {}: crate::{}::{}::{},", ref_table_name_stripped, self.crate_name, fk.ref_table, ref_table_name));
                        
                        b.blank();
                        
                        b.line(f!("#[sqlx(flatten)]"));
                        b.line(f!("pub {}: crate::{}::{}::{},", table_name_stripped, self.crate_name, fk.table, table_name));
                    });

                    table_file.blank();

                    table_file.doc(f!("{}", fk_state.trait_name));
                    table_file.doc_blank();
                    table_file.doc(f!("This is trait is for aggregation of [`{}`] struct", fk_state.row_name));
                    table_file.doc(f!("it will by default be implemented for Vec<{}>", fk_state.row_name));
                    table_file.doc("this struct has options for many to one");
                    table_file.doc("and one to many foreign key relationships");
                    table_file.doc("it also has optional variations of these functions as they will fail if the Vec passed in is empty");
                    table_file.doc_blank();
                    table_file.doc("## Methods");
                    table_file.doc_blank();
                    table_file.doc(f!("## `fn one_to_many(&self) -> Result<{}, &str>`", fk_state.one_to_many_name));
                    table_file.doc_blank();
                    table_file.doc(f!("Returns [`{}`] struct which represents a one to many relationship for this foreign key", fk_state.one_to_many_name));
                    table_file.doc(f!("what that means for you is... one {} to many (list of) {}", ref_table_name_stripped, table_name_stripped));
                    table_file.doc("this function will return error in the case that there is no entries in the Vec passed in (self)");
                    table_file.doc_blank();
                    table_file.doc(f!("## `fn many_to_one(&self) -> Result<{}, &str>`", fk_state.many_to_one_name));
                    table_file.doc_blank();
                    table_file.doc(f!("Returns [`{}`] struct which represents a many to one relationship for this foreign key", fk_state.many_to_one_name));
                    table_file.doc(f!("what that means for you is... one {} to many (list of) {}", table_name_stripped, ref_table_name_stripped));
                    table_file.doc("this function will return error in the case that there is no entries in the Vec passed in (self)");
                    table_file.doc_blank();
                    table_file.doc(f!("## `fn one_to_many_opt(&self) -> Option<{}>`", fk_state.one_to_many_name));
                    table_file.doc_blank();
                    table_file.doc(f!("Returns [`{}`] struct which represents a one to many relationship for this foreign key", fk_state.one_to_many_name));
                    table_file.doc(f!("what that means for you is... one {} to many (list of) {}", ref_table_name_stripped, table_name_stripped));
                    table_file.doc("this function will return None in the case that there is no entries in the Vec passed in (self)");
                    table_file.doc_blank();
                    table_file.doc(f!("## `fn many_to_one_opt(&self) -> Option<{}>`", fk_state.many_to_one_name));
                    table_file.doc_blank();
                    table_file.doc(f!("Returns [`{}`] struct which represents a many to one relationship for this foreign key", fk_state.many_to_one_name));
                    table_file.doc(f!("what that means for you is... one {} to many (list of) {}", table_name_stripped, ref_table_name_stripped));
                    table_file.doc("this function will return None in the case that there is no entries in the Vec passed in (self)");
                    table_file.block(f!("pub trait {} {{", fk_state.trait_name), |b| {
                        b.doc(f!("Returns [`{}`] struct which represents a one to many relationship for this foreign key", fk_state.one_to_many_name));
                        b.doc(f!("what that means for you is... one {} to many (list of) {}", ref_table_name_stripped, table_name_stripped));
                        b.doc("this function will return error in the case that there is no entries in the Vec passed in (self)");
                        b.line(f!("fn one_to_many(self) -> Result<{}, &'static str>;", fk_state.one_to_many_name));

                        b.blank();

                        b.doc(f!("Returns [`{}`] struct which represents a many to one relationship for this foreign key", fk_state.many_to_one_name));
                        b.doc(f!("what that means for you is... one {} to many (list of) {}", table_name_stripped, ref_table_name_stripped));
                        b.doc("this function will return error in the case that there is no entries in the Vec passed in (self)");
                        b.line(f!("fn many_to_one(self) -> Result<{}, &'static str>;", fk_state.many_to_one_name));

                        b.blank();
                        
                        b.doc(f!("Returns [`{}`] struct which represents a one to many relationship for this foreign key", fk_state.one_to_many_name));
                        b.doc(f!("what that means for you is... one {} to many (list of) {}", ref_table_name_stripped, table_name_stripped));
                        b.doc("this function will return None in the case that there is no entries in the Vec passed in (self)");
                        b.line(f!("fn one_to_many_opt(self) -> Option<{}>;", fk_state.one_to_many_name));

                        b.blank();

                        b.doc(f!("Returns [`{}`] struct which represents a many to one relationship for this foreign key", fk_state.many_to_one_name));
                        b.doc(f!("what that means for you is... one {} to many (list of) {}", table_name_stripped, ref_table_name_stripped));
                        b.doc("this function will return None in the case that there is no entries in the Vec passed in (self)");
                        b.line(f!("fn many_to_one_opt(self) -> Option<{}>;", fk_state.many_to_one_name));
                    });

                    table_file.blank();

                    table_file.block(f!("impl {} for Vec<{}> {{", fk_state.trait_name, fk_state.row_name), |b| {
                        b.block(f!("fn one_to_many(mut self) -> Result<{}, &'static str> {{", fk_state.one_to_many_name), |b| {
                            b.block("if self.is_empty() {", |b| {
                                b.line("return Err(\"query returned no rows... cannot aggregate\")");
                            });

                            b.line("let first = self.remove(0);");
                            b.line(f!("let one = first.{};", ref_table_name_stripped));
                            b.line(f!("let many = std::iter::once(first.{}).chain(self.into_iter().map(|r| r.{})).collect();", table_name_stripped, table_name_stripped));
                            b.block_with_paren(f!("Ok({} {{", fk_state.one_to_many_name), |b| {
                                b.line(f!("{}: one,", ref_table_name_stripped));
                                b.line(f!("{}: many", table_name_stripped.pluralise()));
                            });
                        });

                        b.blank();

                        b.block(f!("fn many_to_one(mut self) -> Result<{}, &'static str> {{", fk_state.many_to_one_name), |b| {
                            b.block("if self.is_empty() {", |b| {
                                b.line("return Err(\"query returned no rows... cannot aggregate\")");
                            });

                            b.line("let first = self.remove(0);");
                            b.line(f!("let one = first.{};", table_name_stripped));
                            b.line(f!("let many = std::iter::once(first.{}).chain(self.into_iter().map(|r| r.{})).collect();", ref_table_name_stripped, ref_table_name_stripped));

                            b.block_with_paren(f!("Ok({} {{", fk_state.many_to_one_name), |b| {
                                b.line(f!("{}: one,", table_name_stripped));
                                b.line(f!("{}: many", ref_table_name_stripped.pluralise()));
                            });
                        });

                        b.blank();

                        b.block(f!("fn one_to_many_opt(self) -> Option<{}> {{", fk_state.one_to_many_name), |b| {
                            b.line("self.one_to_many().ok()");
                        });

                        b.blank();

                        b.block(f!("fn many_to_one_opt(self) -> Option<{}> {{", fk_state.many_to_one_name), |b| {
                            b.line("self.many_to_one().ok()");
                        });
                    });
                }

                table_file.blank();

                table_file.block(f!("pub struct {} {{", state.general.builder_struct_name), |b| {
                    b.line(f!("pub letter: {}", state.general.table_struct_name));
                    // for col in &table.cols {
                    //     let rust_type = col.kind.rust_type();
                    //     let field_name = col.field_name.to_case(Case::Snake);
                    
                    // }
                });
                
                table_file.blank();
                
                table_file.block(f!("impl {} {{", state.general.builder_struct_name), |b| {
                    b.block("pub fn new() -> Self {", |b| {
                        b.block("Self {", |b| {
                            b.block(f!("letter: {} {{", state.general.table_struct_name), |b| {
                                for col in &table.cols {
                                    let field_name = col.field_name.to_case(Case::Snake);
                                    let default_val = col.kind.default_value();
                                    
                                    if col.name == "id" {
                                        // because id is always an option
                                        b.line("id: None,");
                                    } else {
                                        if col.not_null == 0 {
                                            b.line(f!("{}: Some({}),", field_name, default_val));
                                        } else {
                                            b.line(f!("{}: {},", field_name, default_val));
                                        }
                                    }
                                }
                            });
                        });
                    });
                    
                    for col in &table.cols {
                        let field_name = col.field_name.to_case(Case::Snake);
                        let rust_type = if col.not_null == 0 {
                            format!("Option<{}>", col.kind.rust_type())
                        } else {
                            col.kind.rust_type().to_string()
                        };
                        
                        b.block(f!("pub fn {}(mut self, new: {}) -> Self {{", field_name, rust_type), |b| {
                            b.line(f!("self.letter.{} = new;", field_name));
                            b.line("self");
                        });
                    }
                    
                    b.block("pub fn populate_fake_data(mut self) -> Self {", |b| {
                        for col in &table.cols {
                            let fake_val = self.fake_data.generate(&table.name, &col, b);
                            let field_name = col.field_name.to_case(Case::Snake);
                            
                            let fake_val = if col.not_null == 0 {
                                if col.name == "id" {
                                    "None".to_string()
                                } else {
                                    format!("Some({})", fake_val)
                                }
                            } else {
                                fake_val
                            };
                            
                            b.line(f!("self.letter.{} = {};", field_name, fake_val));
                        }
                        b.line("self");
                    });
                    
                    b.block(f!("pub fn build(self) -> {} {{", state.general.table_struct_name), |b| {
                        b.line("self.letter");
                    });
                });
            }
            
            table_views.push(table_file);
        }

        Ok(table_views)
    }

    /// Runs the codegen
    /// this function otputs finished codegen to env::var("OUT_DIR")/orm.rs
    /// OUT_DIR is set at compile time
    pub fn run_codegen(&self) -> LuhTwin<()> {
        // the main file goes last
        // first we generate table into a list of RustStringView so we can mod them at the top of this file
        // for maximum readability of codegen

        let mut main_file = RustStringView::new("orm.rs");

        main_file.doc("# Generated ORM module - DONT EDIT");
        main_file.doc_blank();
        main_file.doc("This file is automatically generated by luhorm's build script.");
        main_file.doc("All changes will be overwritten on the next build.");
        main_file.doc_blank();
        main_file.doc("Each table in your database has a corresponding submodule with:");
        main_file.doc("- A main struct representing a row");
        main_file.doc("- A query builder for type-safe queries");
        main_file.doc("- Column enums for compile-time column safety");
        main_file.doc("- Generated join methods for foreign keys");
        main_file.doc("- A builder struct that can generate fake data");
        main_file.doc_blank();
        main_file.doc("## Quick example");
        main_file.doc_blank();
        main_file.doc("```ignore");
        main_file.doc("use orm::users::{Users, UsersQuery};");
        main_file.doc_blank();
        main_file.doc("// direct access");
        main_file.doc("let all_users = Users::get_all(&pool).await?;");
        main_file.doc_blank();
        main_file.doc("// query builder");
        main_file.doc("let adults = Users::query()");
        main_file.doc("    .age_gte(18)");
        main_file.doc("    .fetch_all(&pool)");
        main_file.doc("    .await?;");
        main_file.doc("```");

        main_file.blank();

        let generated_tables = self.run_codegen_for_tables()?;

        for table in &generated_tables {
            main_file.line(f!("pub mod {};", table.name));
        }

        main_file.blank();
        main_file.line("use std::fmt::Display;");

        main_file.blank();

        main_file.doc("# JoinType");
        main_file.doc_blank();
        main_file.doc("This is just a helper enum to differentiate which type of join you would like");
        main_file.doc("you will not have to use this you will use generated methods instead");

        main_file.line("#[derive(Debug, Clone)]");
        main_file.block("pub enum JoinType {", |b| {
            b.line("Inner,");
            b.line("Left,");
            b.line("Right,");
        });

        main_file.blank();

        main_file.doc("# Join");
        main_file.doc_blank();
        main_file.doc("This is just a helper struct to define different joins you might like to do");
        main_file.doc("with different foreign keys inside your tables... you will not use this yourself");
        main_file.doc("you will use this inside of different generated methods");

        main_file.line("#[derive(Debug, Clone)]");
        main_file.block("pub struct Join {", |b| {
            b.line("pub join_type: JoinType,");
            b.line("pub table: String,");
            b.line("pub on_condition: String,");
        });

        main_file.blank();

        main_file.doc("# ColumnRef");
        main_file.doc_blank();
        main_file.doc("You use this struct in combination with different methods from the generated");
        main_file.doc("query methods to build out queries... you can use with different join methods");
        main_file.doc("and where clauses in order to build out queries");
        main_file.doc("NOTE: THIS WILL ALLOW YOU TO BUILD QUERIES WHICH MAKE NO SENSE");
        main_file.doc_blank();
        main_file.doc("## Example");
        main_file.doc_blank();
        main_file.doc("```ignore");
        main_file.doc("Users::query()");
        main_file.doc("    // manual");
        main_file.doc("    .join(Post::NAME, USER::ID.of(), POST::USER_ID.of())");
        main_file.doc("    // generated method");
        main_file.doc("    .join_posts()");
        main_file.doc("    // you can also do it like this");
        main_file.doc("    .where(USER::ID.of().eq(&POST::USER_ID.of()))");
        main_file.doc("    .fetch_all(&pool)");
        main_file.doc("    .await?;");
        main_file.doc("```");
        main_file.doc_blank();
        main_file.doc("## Provided Methods");
        main_file.doc_blank();
        main_file.doc("## `pub fn eq(&self, other: &ColumnRef) -> String`");
        main_file.doc_blank();
        main_file.doc("Generates a query as a string comparing (equality) from self to other (arguement)");
        main_file.doc_blank();
        main_file.doc("## `pub fn eq_value<T: Display>(&self, value: T) -> String`");
        main_file.doc_blank();
        main_file.doc("Generates a query as a string comparing (equality) from self to value (arguement)");
        main_file.doc("where value implements display");
        main_file.doc_blank();
        main_file.doc("## `pub fn gt<T: Display>(&self, value: T) -> String`");
        main_file.doc_blank();
        main_file.doc("Generates a query as a string comparing whether self is greater than value (arguement)");
        main_file.doc("where value implements display");
        main_file.doc_blank();
        main_file.doc("## `pub fn lt<T: Display>(&self, value: T) -> String`");
        main_file.doc_blank();
        main_file.doc("Generates a query as a string comparing whether self is less than value (arguement)");
        main_file.doc("where value implements display");
        main_file.doc_blank();
        main_file.doc("## `pub fn gte<T: Display>(&self, value: T) -> String`");
        main_file.doc_blank();
        main_file.doc("Generates a query as a string comparing whether self is greater than or equal to value (arguement)");
        main_file.doc("where value implements display");
        main_file.doc_blank();
        main_file.doc("## `pub fn lte<T: Display>(&self, value: T) -> String`");
        main_file.doc_blank();
        main_file.doc("Generates a query as a string comparing whether self is less than or equal to value (arguement)");
        main_file.doc("where value implements display");
        // main_file.doc_blank();
        // main_file.doc("## `pub fn like(&self, pattern: &str) -> String`");
        // main_file.doc_blank();
        // main_file.doc("Generates query as a string comparing self to pattern (arguement) as a sql LIKE operation");
        // main_file.doc("where value implements display");

        main_file.line("#[derive(Debug, Clone)]");
        main_file.block("pub struct ColumnRef {", |b| {
            b.line("pub table: String,");
            b.line("pub column: String,");
        });

        main_file.blank();
        
        main_file.block("impl ColumnRef {", |b| {
            b.doc("Generates a query as a string comparing (equality) from self to other (arguement)");
            b.block("pub fn eq(&self, other: &ColumnRef) -> String {", |b| {
                b.line("format!(\"{}.{} = {}.{}\", self.table, self.column, other.table, other.column)");
            });

            b.blank();
            
            b.doc("Generates a query as a string comparing (equality) from self to value (arguement)");
            b.doc("where value implements display");
            b.block("pub fn eq_value<T: Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} = {}\", self.table, self.column, value)");
            });

            b.blank();
            
            b.doc("Generates a query as a string comparing whether self is greater than value (arguement)");
            b.doc("where value implements display");
            b.block("pub fn gt<T: Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} > {}\", self.table, self.column, value)");
            });

            b.blank();
            
            b.doc("Generates a query as a string comparing whether self is less than value (arguement)");
            b.doc("where value implements display");
            b.block("pub fn lt<T: Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} < {}\", self.table, self.column, value)");
            });

            b.blank();
            
            b.doc("Generates a query as a string comparing whether self is greater than or equal to value (arguement)");
            b.doc("where value implements display");
            b.block("pub fn gte<T: Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} >= {}\", self.table, self.column, value)");
            });
            
            b.blank();
            
            b.doc("Generates a query as a string comparing whether self is less than or equal to value (arguement)");
            b.doc("where value implements display");
            b.block("pub fn lte<T: Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} <= {}\", self.table, self.column, value)");
            });

            b.blank();
            
            // b.doc("Generates query as a string comparing self to pattern (arguement) as a sql LIKE operation");
            // b.doc("where value implements display");
            // b.block("pub fn like(&self, pattern: &str) -> String {", |b| {
            //     b.line("format!(\"{}.{} LIKE '{}'\", self.table, self.column, pattern)");
            // });
        });

        // debug

        println!("orm.rs:");
        println!("{}", main_file.buf);

        for t in &generated_tables {
            println!("{}.rs: ", t.name);
            println!("{}", t.buf);
        }

        // prod

        let out_dir = env::var("OUT_DIR")
            .wrap(|| "failed to get out_dir at compile time")?;

        for table in generated_tables {
            let path = Path::new(&out_dir).join(format!("{}.rs", table.name));
            
            fs::write(&path, table.buf)?;
        }

        let main_path = Path::new(&out_dir).join("orm.rs");

        fs::write(&main_path, main_file.buf)?;
        
        Ok(())
    }
}

/// debug tests --- just for now
#[cfg(test)]
mod tests {
    // use super::*;

    use luhtwin::{Encase, LuhTwin, Wrap};
    use sqlx::{postgres::{PgConnectOptions, PgPoolOptions}, sqlite::{SqliteConnectOptions, SqlitePoolOptions}};
    use tokio::runtime;

    use crate::{Codegen, RustStringView};

    #[test]
    fn debug_test() -> LuhTwin<()> {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .wrap(|| "failed to make the compile time async runtime")?;
        
        rt.block_on(async {
            const DB_NAME: &str = "/Users/crack/Bang/luhorm/test.db";
            const MIGR_DIR: &str = "/Users/crack/Bang/luhorm/migr/";

            let opts = SqliteConnectOptions::new()
                .filename(DB_NAME)
                .create_if_missing(true)
                .foreign_keys(true);

            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .connect_with(opts)
                .await
                .wrap(|| "failed to create SQLite pool")?;

            let orm = Codegen::new("orm", pool.clone(), MIGR_DIR, None)
                .await
                .encase(|| "failed to make new orm")?;

            orm.run_codegen()?;
           
            Ok::<(), luhtwin::AnyError>(())
        }).encase(|| "failed to do runtime db work")?;

        Ok(())
    }

    #[test]
    fn other_debug_test() -> LuhTwin<()> {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .wrap(|| "failed to make the compile time async runtime")?;
        
        rt.block_on(async {
            const MIGR_DIR: &str = "/Users/crack/Bang/luhorm/postgres_migr/";

            let opts = PgConnectOptions::new()
                .host("localhost")
                .database("luhorm_testing");

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(opts)
                .await
                .wrap(|| "failed to create SQLite pool")?;

            let orm = Codegen::new("orm", pool.clone(), MIGR_DIR, None)
                .await
                .encase(|| "failed to make new orm")?;

            orm.run_codegen()?;
           
            Ok::<(), luhtwin::AnyError>(())
        }).encase(|| "failed to do runtime db work")?;

        Ok(())
    }

    #[test]
    fn test_string_view_indentation() {
        let mut view = RustStringView::new("test");
        
        view.block("struct Test {", |b| {
            b.line("x: i32,");
            b.line("y: i32");
        });
        
        view.blank();
        
        view.block("impl Test {", |b| {
            b.block("fn new(x: i32, y: i32) -> Self {", |b| {
                b.block("Self {", |b| {
                    b.line("x,");
                    b.line("y,");
                });
            });

            b.write("\n");
            
            b.block("fn sum(self) -> i32 {", |b| {
                b.line("self.x + self.y");
            });
        });
        
        let expected = "\
struct Test {
    x: i32,
    y: i32
}

impl Test {
    fn new(x: i32, y: i32) -> Self {
        Self {
            x,
            y,
        }
    }

    fn sum(self) -> i32 {
        self.x + self.y
    }
}
";
        
        assert_eq!(view.into_string(), expected);
    }

}
