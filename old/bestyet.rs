use std::collections::HashMap;
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value;

use convert_case::{Casing, Case};
use luhtwin::{at, LuhTwin, Wrap};
use sqlx::any::AnyRow;
use sqlx::postgres::{PgColumn, PgRow};
use sqlx::sqlite::{SqliteColumn, SqliteRow};
use sqlx::{Column, ColumnIndex, Database, Decode, Pool, Postgres, Row, Sqlite, SqliteExecutor};

#[derive(Debug, Clone)]
pub struct LuhField<'a> {
    key: &'a str,
    value: Option<&'a Value>,
}

impl<'a> LuhField<'a> {
    pub fn string(self) -> LuhTyped<'a, String> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| v.as_str().map(|s| s.to_string()),
        }
    }

    pub fn i64(self) -> LuhTyped<'a, i64> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| v.as_i64(),
        }
    }

    pub fn f64(self) -> LuhTyped<'a, f64> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| v.as_f64()
        }
    }

    pub fn bool(self) -> LuhTyped<'a, bool> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| v.as_bool(),
        }
    }
}

pub struct LuhTyped<'a, T> {
    key: &'a str,
    value: Option<&'a Value>,
    extract: fn(&Value) -> Option<T>,
}

impl<'a, T> LuhTyped<'a, T> {
    /// Silent, ergonomic path
    pub fn ok(&self) -> Option<T> {
        self.value.and_then(|v| (self.extract)(v))
    }

    /// Explicit failure
    pub fn require(&self) -> LuhTwin<T> {
        self.ok().ok_or_else(|| {
            at!("column '{}' missing or wrong type", self.key).into()
        })
    }
}

#[derive(Debug, Clone)]
pub struct LuhRow {
    cols: HashMap<String, Value>,
}

impl LuhRow {
    pub fn new() -> Self {
        Self { cols: HashMap::new() }
    }

    pub fn from_hashmaps(value: impl IntoIterator<Item = HashMap<String, Value>>) -> Vec<Self> {
        value
            .into_iter()
            .map(LuhRow::from)
            .collect()
    }

    pub fn value<'a>(&'a self, key: &'a str) -> LuhField<'a> {
        LuhField {
            key,
            value: self.cols.get(key),
        }
    }

    pub fn insert<S: Into<String>>(&mut self, key: S, value: Value) {
        self.cols.insert(key.into(), value);
    }
}

impl From<HashMap<String, Value>> for LuhRow {
    fn from(value: HashMap<String, Value>) -> Self {
        Self { cols: value }
    }
}

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
    pub fn rust_type(&self) -> &'static str {
        match self {
            DbType::Integer => "i32",
            DbType::BigInt => "i64",
            DbType::Real => "f64",
            DbType::Boolean => "bool",
            DbType::Text => "String",
            DbType::Blob => "Vec<u8>",
            DbType::Timestamp => "chrono::NaiveDateTime",
            DbType::Json => "serde_json::Value",
            DbType::Numeric => "rust_decimal::Decimal",
            // fix this just idk how the versioning of uuid crate works cba right this second
            DbType::Uuid => "String",
            DbType::Unknown(_) => "String",
        }
    }

    pub fn cow_rust_type(&self) -> (&'static str, bool) {
        match self {
            DbType::Text => ("Cow<'a, str>", true),
            DbType::Blob => ("Cow<'a, [u8]>", true),
            _ => (self.rust_type(), false),
        }
    }
}

fn strip_parens(s: &str) -> &str {
    s.split('(').next().unwrap_or(s)
}


pub trait SqlNormalise {
    fn sql_normalise(&self) -> String;
}

impl SqlNormalise for &str {
    fn sql_normalise(&self) -> String {
        strip_parens(self)
            .trim()
            .to_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }
}

pub struct ColumnData {
    name: String,
    kind: DbType,
    field_name: String,
    not_null: i64,
}

impl ColumnData {
    fn is_numeric(&self) -> bool {
        matches!(
            self.kind,
            DbType::Integer | DbType::BigInt | DbType::Real
        )
    }

    fn is_string_like(&self) -> bool {
        matches!(
            self.kind,
            DbType::Text | DbType::Blob
        )
    }
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

pub struct TableData {
    pub cols: Vec<ColumnData>,
    pub foreign_keys: Vec<ForeignKey>,
    pub name: String
}

pub fn basic_value_to_json(row: &AnyRow, col_name: &str) -> Value {
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        Value::Number(v.into())
    } else if let Ok(v) = row.try_get::<f64, _>(col_name) {
        Value::Number(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)))
    } else if let Ok(v) = row.try_get::<String, _>(col_name) {
        Value::String(v)
    } else if let Ok(v) = row.try_get::<bool, _>(col_name) {
        Value::Bool(v)
    } else if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
        Value::Array(v.iter().map(|b| Value::Number((*b).into())).collect())
    } else {
        Value::Null
    }
}

pub trait TypeMapper {
    type Row: Row;
    type Column: Column;

    // fn create_migrations_table()

    fn master_table_columns_from_rows<'a>(rows: impl IntoIterator<Item = &'a LuhRow>) -> LuhTwin<Vec<ColumnData>>;
    fn foreign_keys_from_rows<'a>(
        table: &str,
        rows: impl IntoIterator<Item = &'a LuhRow>,
    ) -> LuhTwin<Vec<ForeignKey>>;
    fn parse_type(decl: &str) -> DbType;

    fn value_to_json(
        &self,
        row: &Self::Row,
        col: &Self::Column,
    ) -> Value;

    fn placeholder(idx: usize) -> String;
    fn uses_numbered_placeholders() -> bool;
    fn sqlx_type() -> String;
}

pub struct SqliteMapper;

impl TypeMapper for SqliteMapper {
    type Row = SqliteRow;
    type Column = SqliteColumn;

    fn placeholder(_idx: usize) -> String {
        "?".to_string()
    }

    fn uses_numbered_placeholders() -> bool {
        false
    }

    fn sqlx_type() -> String {
        "SqlitePool".to_string()
    }

    fn value_to_json(&self, row: &Self::Row, col: &Self::Column) -> Value {
        let name = col.name();
        
        basic_value_to_json(row, name)
    }

    fn master_table_columns_from_rows<'a>(rows: impl IntoIterator<Item = &'a LuhRow>) -> LuhTwin<Vec<ColumnData>> {
        rows.into_iter()
            .map(|c| {
                let name = c.value("name")
                    .string()
                    .require()
                    .wrap(|| "column 'name' must be a string")?;

                Ok(ColumnData {
                    field_name: name.to_case(Case::Snake),
                    name,
                    kind: SqliteMapper::parse_type(c.value("type")
                        .string()
                        .require()
                        .wrap(|| "column 'type' must be a string")?.as_ref()),
                    not_null: c.value("notnull")
                        .i64()
                        .require()
                        .wrap(|| "column 'notnull' must be an integer")?,
                })
            })
            .collect()
    }

    fn foreign_keys_from_rows<'a>(
        table: &str,
        rows: impl IntoIterator<Item = &'a LuhRow>,
    ) -> LuhTwin<Vec<ForeignKey>> {
        rows.into_iter()
            .map(|r| {
                Ok(ForeignKey {
                    table: table.to_string(),
                    column: r.value("from").string().require()?,
                    ref_table: r.value("table").string().require()?,
                    ref_column: r.value("to").string().require()?,
                    on_update: r.value("on_update").string().ok().map(|s| s.to_owned()),
                    on_delete: r.value("on_delete").string().ok().map(|s| s.to_owned()),
                })
            })
            .collect()
    }

    fn parse_type(decl: &str) -> DbType {
        let d = decl.sql_normalise();

        if d.contains("int") {
            DbType::Integer
        } else if d.contains("char") || d.contains("clob") || d.contains("text") {
            DbType::Text
        } else if d.contains("blob") {
            DbType::Blob
        } else if d.contains("real") || d.contains("floa") || d.contains("doub") {
            DbType::Real
        } else if d.contains("bool") {
            DbType::Boolean
        } else if d.contains("json") {
            DbType::Json
        } else if d.contains("date") || d.contains("time") {
            DbType::Timestamp
        } else {
            DbType::Numeric
        }
    }
}

pub struct PostgresMapper;

impl TypeMapper for PostgresMapper {
    type Row = PgRow;
    type Column = PgColumn;

    fn placeholder(idx: usize) -> String {
        format!("${}", idx + 1)
    }

    fn sqlx_type() -> String {
        "PgPool".to_string()
    }

    fn uses_numbered_placeholders() -> bool {
        true
    }

    fn value_to_json(&self, row: &Self::Row, col: &Self::Column) -> Value {
        let name = col.name();
        
        if let Ok(v) = row.try_get::<i64, _>(name) {
            Value::Number(v.into())
        } else if let Ok(v) = row.try_get::<f64, _>(name) {
            Value::Number(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)))
        } else if let Ok(v) = row.try_get::<String, _>(name) {
            Value::String(v)
        } else if let Ok(v) = row.try_get::<bool, _>(name) {
            Value::Bool(v)
        } else if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(name) {
            Value::String(v.to_string())
        } else if let Ok(v) = row.try_get::<chrono::NaiveDate, _>(name) {
            Value::String(v.to_string())
        } else if let Ok(v) = row.try_get::<Vec<u8>, _>(name) {
            Value::Array(v.iter().map(|b| Value::Number((*b).into())).collect())
        } else {
            Value::Null
        }
    }

    fn master_table_columns_from_rows<'a>(rows: impl IntoIterator<Item = &'a LuhRow>) -> LuhTwin<Vec<ColumnData>> {
        rows.into_iter()
            .map(|c| {
                let name = c.value("column_name").string().require()
                    .wrap(|| "column 'name' must be a string")?;
                let kind = PostgresMapper::parse_type(c.value("data_type").string().require()
                    .wrap(|| "column 'type' must be a string")?.as_ref());
                let not_null = if c.value("is_nullable").string().require()
                    .wrap(|| "column 'notnull' must be an integer")? == "NO" {
                    1
                } else {
                    0
                };

                Ok(ColumnData {
                    field_name: name.to_case(Case::Snake),
                    name,
                    kind,
                    not_null,
                })
            })
            .collect()
    }

    fn foreign_keys_from_rows<'a>(
        table: &str,
        rows: impl IntoIterator<Item = &'a LuhRow>,
    ) -> LuhTwin<Vec<ForeignKey>> {
        rows.into_iter()
            .map(|r| {
                Ok(ForeignKey {
                    table: table.to_string(),
                    column: r.value("column_name").string().require()?,
                    ref_table: r.value("foreign_table_name").string().require()?,
                    ref_column: r.value("foreign_column_name").string().require()?,
                    on_update: r.value("update_rule").string().ok().map(|s| s.to_owned()),
                    on_delete: r.value("delete_rule").string().ok().map(|s| s.to_owned()),
                })
            })
            .collect()
    }

    fn parse_type(ty: &str) -> DbType {
        let t = ty.sql_normalise();

        match t.as_str() {
            "int2" | "smallint" => DbType::Integer,
            "int4" | "integer" => DbType::Integer,
            "int8" | "bigint" => DbType::BigInt,
            
            "float4" | "real" => DbType::Real,
            "float8" | "double precision" => DbType::Real,
            
            "numeric" | "decimal" => DbType::Numeric,
            
            "bool" | "boolean" => DbType::Boolean,
            
            "text" | "varchar" | "character varying" | "char" | "character" => DbType::Text,
            
            "bytea" => DbType::Blob,
            
            "timestamp" => DbType::Timestamp,
            "timestamptz" | "timestamp with time zone" => DbType::Timestamp,
            
            "json" | "jsonb" => DbType::Json,
            
            "uuid" => DbType::Uuid,
            
            other => DbType::Unknown(other.into()),
        }
    }
}

pub enum LuhParam<'a> {
    I64(i64),
    F64(f64),
    String(&'a str),
    Bool(bool),
}

type Params<'a> = Option<&'a [LuhParam<'a>]>;

#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    type TM: TypeMapper;

    fn type_mapper(&self) -> Self::TM;
    async fn execute_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<u64>;
    async fn fetch_all_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<Vec<LuhRow>>;
    async fn get_master_table(&self) -> LuhTwin<Vec<TableData>>;
}

#[async_trait::async_trait]
impl Backend for Pool<Sqlite> {
    type TM = SqliteMapper;

    fn type_mapper(&self) -> Self::TM {
        SqliteMapper
    }

    async fn fetch_all_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<Vec<LuhRow>> {
        let mut q = sqlx::query(query);
        
        if let Some(params) = params {
            for p in params {
                match p {
                    LuhParam::I64(v) => q = q.bind(v),
                    LuhParam::F64(v) => q = q.bind(v),
                    LuhParam::String(v) => q = q.bind(v),
                    LuhParam::Bool(v) => q = q.bind(v),
                }
            }
        }
        
        let rows = q.fetch_all(self)
           .await
           .wrap(|| format!("failed to fetch_all with params. query: {}", query))?;

        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let mut map = HashMap::new();

            for col in row.columns() {
                let value = self.type_mapper().value_to_json(&row, col);
                map.insert(col.name().to_string(), value);
            }

            results.push(map);
        }

        Ok(LuhRow::from_hashmaps(results))
    }

    async fn execute_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<u64> {
        let mut q = sqlx::query(query);

        if let Some(params) = params {
            for p in params {
                match p {
                    LuhParam::I64(v) => q = q.bind(v),
                    LuhParam::F64(v) => q = q.bind(v),
                    LuhParam::String(v) => q = q.bind(v),
                    LuhParam::Bool(v) => q = q.bind(v),
                }
            }
        }

        let res = q.execute(self)
            .await
            .wrap(|| format!("failed to execute query: {}", query))?;

        Ok(res.rows_affected())
    }

    async fn get_master_table(&self) -> LuhTwin<Vec<TableData>> {
        let rows = self.fetch_all_raw("SELECT name FROM sqlite_master WHERE type='table'", None)
            .await
            .wrap(|| "failed to fetch tables")?;

        let table_names: Vec<String> = rows
            .into_iter()
            .map(|row| row.value("name").string().ok())
            .flatten()
            .filter(|name| !name.starts_with("sqlite_") && !name.starts_with("_sqlx_") && !name.starts_with("_luhorm"))
            .collect();

        let mut tables: Vec<TableData> = vec![];

        for table in table_names {
            let cols = self.fetch_all_raw(&format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\"")), None)
                .await
                .wrap(|| format!("failed to get columns for table \"{}\"", table))?;

            let fk_rows = self.fetch_all_raw(
                &format!("PRAGMA foreign_key_list(\"{}\")", table.replace('"', "\"\"")),
                None
            ).await?;
            
            let foreign_keys = Self::TM::foreign_keys_from_rows(&table, &fk_rows)?;

            println!("{:?}", foreign_keys);

            tables.push(TableData {
                cols: Self::TM::master_table_columns_from_rows(&cols)?,
                foreign_keys,
                name: table
            });
        };

        Ok(tables)
    }
}

#[async_trait::async_trait]
impl Backend for Pool<Postgres> {
    type TM = PostgresMapper;

    fn type_mapper(&self) -> Self::TM {
        PostgresMapper
    }

    async fn fetch_all_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<Vec<LuhRow>> {
        let mut q = sqlx::query(query);
        
        if let Some(params) = params {
            for p in params {
                match p {
                    LuhParam::I64(v) => q = q.bind(v),
                    LuhParam::F64(v) => q = q.bind(v),
                    LuhParam::String(v) => q = q.bind(v),
                    LuhParam::Bool(v) => q = q.bind(v),
                }
            }
        }
        
        let rows = q.fetch_all(self)
           .await
           .wrap(|| format!("failed to fetch_all with params. query: {}", query))?;

        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let mut map = HashMap::new();

            for col in row.columns() {
                let value = self.type_mapper().value_to_json(&row, col);
                map.insert(col.name().to_string(), value);
            }

            results.push(map);
        }

        Ok(LuhRow::from_hashmaps(results))
    }

    async fn execute_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<u64> {
        let mut q = sqlx::query(query);

        if let Some(params) = params {
            for p in params {
                match p {
                    LuhParam::I64(v) => q = q.bind(v),
                    LuhParam::F64(v) => q = q.bind(v),
                    LuhParam::String(v) => q = q.bind(v),
                    LuhParam::Bool(v) => q = q.bind(v),
                }
            }
        }

        let res = q.execute(self)
            .await
            .wrap(|| format!("failed to execute query: {}", query))?;

        Ok(res.rows_affected())
    }

    async fn get_master_table(&self) -> LuhTwin<Vec<TableData>> {
        let rows = self.fetch_all_raw(
            "SELECT table_name
             FROM information_schema.tables
             WHERE table_schema = 'public'
             AND table_type = 'BASE TABLE';",
            None
        )
            .await
            .wrap(|| "failed to fetch tables")?;
        
        let table_names: Vec<String> = rows
            .into_iter()
            .map(|row| row.value("table_name").string().ok())
            .flatten()
            .collect();
        
        let mut tables: Vec<TableData> = vec![];
        
        for table in table_names {
            let cols = self.fetch_all_raw(&format!(
                "SELECT column_name, data_type, is_nullable, column_default
                 FROM information_schema.columns
                 WHERE table_schema = 'public'
                 AND table_name = '{}';", table.replace('\'', "''")
            ), None).await.wrap(|| format!("failed to get columns for table \"{}\"", table))?;


            let fk_rows = self.fetch_all_raw(
                "SELECT kcu.column_name, ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name, rc.update_rule, rc.delete_rule
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name
                 JOIN information_schema.constraint_column_usage ccu
                 ON ccu.constraint_name = tc.constraint_name
                 JOIN information_schema.referential_constraints rc
                 ON rc.constraint_name = tc.constraint_name
                 WHERE tc.constraint_type = 'FOREIGN KEY'
                 AND tc.table_schema = 'public'
                 AND tc.table_name = $1;",
                Some(&[LuhParam::String(&table)])
            ).await?;

            let foreign_keys = Self::TM::foreign_keys_from_rows(&table, &fk_rows)?;

            println!("{:?}", foreign_keys);
            
            tables.push(TableData {
                cols: Self::TM::master_table_columns_from_rows(&cols)?,
                foreign_keys,
                name: table,
            });
        }
        
        Ok(tables)
    }
}

// pub async fn migrate<D: LuhOrm>(pool: &D, migr_dir: impl Into<PathBuf>) -> LuhTwin<()> {
//     let migr_dir: PathBuf = migr_dir.into();

//     let create_table_sql = if pool.kind().uses_numbered_placeholders() {
//         // postgres
//         "CREATE TABLE IF NOT EXISTS _luhorm_migrations (
//              id INTEGER GENERATED AS IDENTITY PRIMARY KEY,
//              filename TEXT NOT NULL UNIQUE,
//              hash TEXT NOT NULL,
//              applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
//          );"
//     } else {
//         "CREATE TABLE IF NOT EXISTS _luhorm_migrations (
//              id INTEGER PRIMARY KEY AUTOINCREMENT,
//              filename TEXT NOT NULL UNIQUE,
//              hash TEXT NOT NULL,
//              applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
//          );"
//     };
    
//     pool.execute_raw(create_table_sql)
//         .await
//         .wrap(|| "failed to run migrations table schema sql")?;

//     let applied: Vec<(String, String)> = pool
//         .fetch_all_raw("SELECT filename, hash FROM _luhorm_migrations ORDER BY id")
//         .await
//         .wrap(|| "failed to query the pool for applied migrations")?
//         .into_iter()
//         .map(|row| {
//             let filename = row.value("filename").string().require()
//                 .wrap(|| "failed to get filename")?;
//             let hash = row.value("hash").string().require()
//                 .wrap(|| "failed to get hash")?;
//             Ok((filename, hash))
//         })
//         .collect::<LuhTwin<Vec<_>>>()?;

//     let mut entries: Vec<_> = std::fs::read_dir(&migr_dir)
//         .wrap(|| "failed to read migrations dir")?
//         .filter_map(Result::ok)
//         .filter(|e| {
//             let path = e.path();

//             if path.extension().map(|ext| ext != "sql").unwrap_or(true) {
//                 return false;
//             }

//             if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
//                 if fname.starts_with('.') || fname.starts_with('#') || fname.ends_with('~') {
//                     return false;
//                 }
//             }
//             true
//         })
//         .collect();
    
//     entries.sort_by_key(|e| e.file_name());
    
//     for (i, (applied_file, applied_hash)) in applied.iter().enumerate() {
//         if let Some(entry) = entries.get(i) {
//             let expected_file = entry.file_name().to_string_lossy().to_string();
            
//             if applied_file != &expected_file {
//                 return Err(at!(
//                     "migration order mismatch! applied: {}, expected: {}",
//                     applied_file, expected_file
//                 ).into());
//             }
            
//             let sql = std::fs::read(entry.path())
//                 .wrap(|| format!("failed to read migration file {}", expected_file))?;
            
//             let mut hasher = Sha256::new();
//             hasher.update(sql);

//             let file_hash = format!("{:x}", hasher.finalize());
            
//             if &file_hash != applied_hash {
//                 return Err(at!(
//                     "migration file changed after being applied: {}",
//                     expected_file
//                 ).into());
//             }
//         } else {
//             return Err(at!(
//                 "database has more applied migrations than files on disk. extra: {}",
//                 applied_file
//             ).into());
//         }
//     }

//     for entry in entries.iter().skip(applied.len()) {
//         let filename = entry.file_name().to_string_lossy().to_string();
//         let sql_bytes = std::fs::read(entry.path())
//             .wrap(|| format!("failed to read migration file {}", filename))?;

//         let mut hasher = Sha256::new();
//         hasher.update(&sql_bytes);
//         let file_hash = format!("{:x}", hasher.finalize());
        
//         let sql_str = String::from_utf8(sql_bytes)
//             .wrap(|| format!("failed to convert {} to UTF-8", filename))?;
        
//         let statements = split_sql_statements(&sql_str);
        
//         for (idx, statement) in statements.iter().enumerate() {
//             pool.execute_raw(statement)
//                 .await
//                 .wrap(|| format!("failed to execute migration: {} (statement {})", filename, idx + 1))?;
//         }

//         let kind = pool.kind();

//         pool.execute_with_params(
//             &format!("INSERT INTO _luhorm_migrations (filename, hash) VALUES ({}, {})", 
//                 kind.placeholder(0), kind.placeholder(1)),
//             &[
//                 LuhValue::String(&filename),
//                 LuhValue::String(&file_hash)
//             ]
//         ).await.wrap(|| "failed to insert new migration to table")?;

//         println!("applied migration: {}", filename);
//     }

//     Ok(())
// }

#[cfg(test)]
mod tests {
    // use super::*;

    use luhtwin::{Encase, LuhTwin, Wrap};
    use sqlx::{postgres::{PgConnectOptions, PgPoolOptions}, sqlite::{SqliteConnectOptions, SqlitePoolOptions}};
    use tokio::runtime;

    use crate::Backend;

    #[test]
    fn sqlite_test() -> LuhTwin<()> {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .wrap(|| "failed to make the compile time async runtime")?;
        
        rt.block_on(async {
            const DB_NAME: &str = "/Users/crack/Bang/luhorm/test.db";
            // const MIGR_DIR: &str = "/Users/crack/Bang/luhorm/migr/";

            let opts = SqliteConnectOptions::new()
                .filename(DB_NAME)
                .create_if_missing(true)
                .foreign_keys(true);

            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .connect_with(opts)
                .await
                .wrap(|| "failed to create SQLite pool")?;

            pool.get_master_table()
                .await
                .wrap(|| "failed to get master table")?;

            // let orm = Codegen::new_sqlite(pool.clone(), MIGR_DIR)
            //     .await
            //     .encase(|| "failed to make new orm")?;

            // orm.run_codegen().await?;
           
            Ok::<(), luhtwin::AnyError>(())
        }).encase(|| "failed to do runtime db work")?;

        Ok(())
    }

    #[test]
    fn postgres_test() -> LuhTwin<()> {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .wrap(|| "failed to make the compile time async runtime")?;
        
        rt.block_on(async {
            const DB_NAME: &str = "/Users/crack/Bang/luhorm/test.db";
            // const MIGR_DIR: &str = "/Users/crack/Bang/luhorm/migr/";

            let opts = PgConnectOptions::new()
                .host("localhost")
                .database("luhorm_testing");

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(opts)
                .await
                .wrap(|| "failed to create Postgres pool")?;

            pool.get_master_table()
                .await
                .wrap(|| "failed to get master table")?;

            // let orm = Codegen::new_sqlite(pool.clone(), MIGR_DIR)
            //     .await
            //     .encase(|| "failed to make new orm")?;

            // orm.run_codegen().await?;
           
            Ok::<(), luhtwin::AnyError>(())
        }).encase(|| "failed to do runtime db work")?;

        Ok(())
    }
}
