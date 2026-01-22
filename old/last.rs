/*



*/

use std::{borrow::Cow, collections::HashMap, env, fs, path::{Path, PathBuf}};

use convert_case::{Casing, Case};
use luhtwin::{at, Encase, LuhTwin, Wrap};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgRow, sqlite::{SqliteColumn, SqliteRow}, Database, Decode, Pool, Postgres, Row, Sqlite, Type};

#[derive(Eq, PartialEq, PartialOrd)]
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
            // temporary
            DbType::Numeric => "f64",
            // DbType::Numeric => "rust_decimal::Decimal",
            // fix this just idk how the versioning of uuid crate works cba right this second
            DbType::Uuid => "String",
            DbType::Unknown(_) => "String",
        }
    }
}

pub trait StripParens {
    fn sql_strip_parens(&self) -> String;
}

impl StripParens for &str {
    fn sql_strip_parens(&self) -> String {
        self.split('(').next().unwrap_or(self)
            .trim()
            .to_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
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

pub struct ColumnData {
    name: String,
    kind: DbType,
    field_name: String,
    not_null: i64,
}

impl ColumnData {
    pub fn rust_type(&self) -> String {
        self.kind.rust_type().to_string()
    }

    pub fn cow_rust_type(&self) -> (String, bool) {
        match self.kind {
            DbType::Text => ("Cow<'static, str>".to_string(), true),
            // DbType::Blob => ("Cow<'static, [u8]>".to_string(), true),
            _ => (self.rust_type(), false),
        }
    }

    fn is_numeric(&self) -> bool {
        matches!(
            self.kind,
            DbType::Integer | DbType::BigInt | DbType::Real | DbType::Numeric
        )
    }

    fn is_string_like(&self) -> bool {
        matches!(
            self.kind,
            DbType::Text | DbType::Blob
        )
    }
}

pub struct TableData {
    pub cols: Vec<ColumnData>,
    pub fks: Vec<ForeignKey>,
    pub name: String
}

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

    pub fn raw(self) -> LuhTyped<'a, Vec<u8>> {
        LuhTyped {
            key: self.key,
            value: self.value,
            extract: |v| serde_json::to_vec(v).ok()
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

impl From<SqliteRow> for LuhRow {
    fn from(row: SqliteRow) -> Self {
        use sqlx::{Column, Row};
        
        let mut map = HashMap::new();
        
        for col in row.columns() {
            let col_name = col.name();
            
            let value = if let Ok(v) = row.try_get::<i64, _>(col_name) {
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
            };
            
            map.insert(col_name.to_string(), value);
        }
        
        Self { cols: map }
    }
}

impl From<PgRow> for LuhRow {
    fn from(row: PgRow) -> Self {
        use sqlx::{Column, Row};
        
        let mut map = HashMap::new();
        
        for col in row.columns() {
            let col_name = col.name();
            
            let value = if let Ok(v) = row.try_get::<i64, _>(col_name) {
                Value::Number(v.into())
            } else if let Ok(v) = row.try_get::<i32, _>(col_name) {
                Value::Number(v.into())
            } else if let Ok(v) = row.try_get::<f64, _>(col_name) {
                Value::Number(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)))
            } else if let Ok(v) = row.try_get::<String, _>(col_name) {
                Value::String(v)
            } else if let Ok(v) = row.try_get::<bool, _>(col_name) {
                Value::Bool(v)
            } else if let Ok(v) = row.try_get::<serde_json::Value, _>(col_name) {
                v
            } else if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
                Value::Array(v.iter().map(|b| Value::Number((*b).into())).collect())
            } else {
                Value::Null
            };
            
            map.insert(col_name.to_string(), value);
        }
        
        Self { cols: map }
    }
}

impl From<HashMap<String, Value>> for LuhRow {
    fn from(value: HashMap<String, Value>) -> Self {
        Self { cols: value }
    }
}

pub trait SqlNormalise {
    fn sql_normalise(&self) -> String;
}

impl SqlNormalise for &str {
    fn sql_normalise(&self) -> String {
        self.sql_strip_parens()
            .trim()
            .to_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }
}

pub trait TypeMapper {
    // type Row: Row;
    // type Column: Column;
    // fn create_migrations_table()

    /// just a little document for this rn as not doing docs till later
    /// table name must be _luhorm_migrations
    /// id needs to be the primary key/equivelent and must autoincrement
    /// filename - text
    /// hash - text
    /// applied_at - timestamp, default must be current_timestamp (important code expects that!!!)
    fn create_migration_table(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS _luhorm_migrations (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             filename TEXT NOT NULL UNIQUE,
             hash TEXT NOT NULL,
             applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
         );"
    }

    fn master_table_columns_from_rows<'a>(rows: impl IntoIterator<Item = &'a LuhRow>) -> LuhTwin<Vec<ColumnData>>;
    fn foreign_keys_from_rows<'a>(
        table: &str,
        rows: impl IntoIterator<Item = &'a LuhRow>,
    ) -> LuhTwin<Vec<ForeignKey>>;

    fn parse_type(decl: &str) -> DbType;

    fn placeholder(&self, idx: usize) -> String;
    fn uses_numbered_placeholders(&self) -> bool;
    fn sqlx_type(&self) -> String;
}

pub struct SqliteMapper;

impl TypeMapper for SqliteMapper {
    fn placeholder(&self, _idx: usize) -> String {
        "?".to_string()
    }

    fn uses_numbered_placeholders(&self) -> bool {
        false
    }

    fn sqlx_type(&self) -> String {
        "SqlitePool".to_string()
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
    fn create_migration_table(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS _luhorm_migrations (
             id SERIAL PRIMARY KEY,
             filename TEXT NOT NULL UNIQUE,
             hash TEXT NOT NULL,
             applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
         );"
    }

    fn placeholder(&self, idx: usize) -> String {
        format!("${}", idx + 1)
    }

    fn sqlx_type(&self) -> String {
        "PgPool".to_string()
    }

    fn uses_numbered_placeholders(&self) -> bool {
        true
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

    fn type_mapper(&self) -> &Self::TM;
    async fn execute_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<u64>;
    async fn fetch_all_raw(&self, query: &str, params: Params<'_>) -> LuhTwin<Vec<LuhRow>>;
    async fn get_master_table(&self) -> LuhTwin<Vec<TableData>>;
}

#[async_trait::async_trait]
impl Backend for Pool<Sqlite> {
    type TM = SqliteMapper;

    fn type_mapper(&self) -> &Self::TM {
        &SqliteMapper
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

        Ok(rows.into_iter().map(LuhRow::from).collect())
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
                fks: foreign_keys,
                name: table
            });
        };

        Ok(tables)
    }
}

#[async_trait::async_trait]
impl Backend for Pool<Postgres> {
    type TM = PostgresMapper;

    fn type_mapper(&self) -> &Self::TM {
        &PostgresMapper
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

        Ok(rows.into_iter().map(LuhRow::from).collect())
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
            .filter(|name| !name.starts_with("postgres_") && !name.starts_with("_sqlx_") && !name.starts_with("_luhorm"))
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
                fks: foreign_keys,
                name: table,
            });
        }
        
        Ok(tables)
    }
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut string_char = ' ';
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        // start/end block comments
        if !in_string && !in_line_comment && !in_block_comment && ch == '/' && chars.peek() == Some(&'*') {
            in_block_comment = true;
            current.push(ch);
            current.push(chars.next().unwrap());
            continue;
        }
        if in_block_comment {
            current.push(ch);
            if ch == '*' && chars.peek() == Some(&'/') {
                current.push(chars.next().unwrap());
                in_block_comment = false;
            }
            continue;
        }

        // line comments
        if !in_string && !in_line_comment && ch == '-' && chars.peek() == Some(&'-') {
            in_line_comment = true;
            current.push(ch);
            current.push(chars.next().unwrap());
            continue;
        }
        if in_line_comment {
            current.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        // strings
        if (ch == '\'' || ch == '"') && !in_string {
            in_string = true;
            string_char = ch;
            current.push(ch);
            continue;
        } else if in_string && ch == string_char {
            in_string = false;
            current.push(ch);
            continue;
        }

        // statement delimiter
        if ch == ';' && !in_string && !in_line_comment && !in_block_comment {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current.clear();
            continue;
        }

        current.push(ch);
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

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

    for entry in entries.iter().skip(applied.len()) {
        let filename = entry.file_name().to_string_lossy().to_string();
        let sql_bytes = std::fs::read(entry.path())
            .wrap(|| format!("failed to read migration file {}", filename))?;

        let mut hasher = Sha256::new();
        hasher.update(&sql_bytes);
        let file_hash = format!("{:x}", hasher.finalize());
        
        let sql_str = String::from_utf8(sql_bytes)
            .wrap(|| format!("failed to convert {} to UTF-8", filename))?;
        
        let statements = split_sql_statements(&sql_str);
        
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

#[derive(Debug, Default)]
pub struct RustStringView {
    buf: String,
    indent: usize,
    indent_with: &'static str,
}

impl RustStringView {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            indent: 0,
            indent_with: "    ", // 4 spaces
        }
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

    /// Write a block:
    /// block("impl Foo {", |b| {...})
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

    pub fn try_block<F>(&mut self, header: &str, f: F) -> LuhTwin<&mut Self>
    where
        F: FnOnce(&mut Self) -> LuhTwin<()>,
    {
        self.line(header);
        self.indent();
        f(self)?;
        self.dedent();
        self.line("}");
        Ok(self)
    }
}

macro_rules! f {
    ($($args:tt)*) => {
        Cow::Owned(format!($($args)*))
    };
}

pub struct Codegen<D: Backend> {
    pool: D,
}

impl<D: Backend> Codegen<D> {
    pub async fn new(pool: D, migr_dir: impl Into<PathBuf>) -> LuhTwin<Self> {
        let migr_dir: PathBuf = migr_dir.into();

        if !migr_dir.exists() && !migr_dir.is_dir() {
            return Err(at!("migr dir doesn't exist or is a directory: {}", migr_dir.display()).into())
        }

        migrate(&pool, migr_dir)
            .await
            .encase(|| "failed to migrate when making new LuhOrm")?;

        let x = Self {
            pool,
        };

        Ok(x)
    }

    pub async fn run_codegen(&self) -> LuhTwin<()> {
        let tables: Vec<TableData> = self.pool.get_master_table()
            .await
            .wrap(|| "failed to get master table in codegen")?;

        let mut generated = RustStringView::new();

        generated.line("use serde::Serialize;");
        generated.line("use sqlx::FromRow;");
        generated.line("use std::borrow::Cow;");

        // prerequesites
        generated.line("#[derive(Debug, Clone)]");
        generated.block("pub enum JoinType {", |b| {
            b.line("Inner,");
            b.line("Left,");
            b.line("Right,");
        });

        generated.line("#[derive(Debug, Clone)]");
        generated.block("pub struct Join {", |b| {
            b.line("pub join_type: JoinType,");
            b.line("pub table: String,");
            b.line("pub on_condition: String,");
        });

        generated.line("#[derive(Debug, Clone)]");
        generated.block("pub struct ColumnRef {", |b| {
            b.line("pub table: String,");
            b.line("pub column: String,");
        });
        generated.blank();
        
        generated.block("impl ColumnRef {", |b| {
            b.block("pub fn eq(&self, other: &ColumnRef) -> String {", |b| {
                b.line("format!(\"{}.{} = {}.{}\", self.table, self.column, other.table, other.column)");
            });
            
            b.block("pub fn eq_value<T: std::fmt::Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} = {}\", self.table, self.column, value)");
            });
            
            b.block("pub fn gt<T: std::fmt::Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} > {}\", self.table, self.column, value)");
            });
            
            b.block("pub fn lt<T: std::fmt::Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} < {}\", self.table, self.column, value)");
            });
            
            b.block("pub fn gte<T: std::fmt::Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} >= {}\", self.table, self.column, value)");
            });
            
            b.block("pub fn lte<T: std::fmt::Display>(&self, value: T) -> String {", |b| {
                b.line("format!(\"{}.{} <= {}\", self.table, self.column, value)");
            });
            
            b.block("pub fn like(&self, pattern: &str) -> String {", |b| {
                b.line("format!(\"{}.{} LIKE '{}'\", self.table, self.column, pattern)");
            });
        });
        generated.blank();
        
        for table in tables {
            let field_names: Vec<String> = table.cols.iter()
                .filter(|c| c.name != "id")
                .map(|c| c.name.clone())
                .collect();

            let rust_field_names: Vec<String> = field_names.iter()
                .map(|f| f.to_case(Case::Snake))
                .collect();

            let field_list = field_names.join(", ");
            let placeholders: String = (0..field_names.len())
                .map(|i| self.pool.type_mapper().placeholder(i))
                .collect::<Vec<_>>()
                .join(", ");

            let select_fields = table.cols.iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(", ");

            let update_fields = field_names
                .iter()
                .enumerate()
                .map(|(i, f)| format!("{} = {}", f, self.pool.type_mapper().placeholder(i)))
                .collect::<Vec<_>>()
                .join(", ");

            let id_col = table.cols.iter()
                .find(|c| c.name == "id")
                .ok_or_else(|| at!("table '{}' must have an 'id' column", table.name))?;
            
            // use a modified version for type generation only
            let id_type = id_col.kind.rust_type();
            let id_type = if id_col.not_null == 0 {
                format!("Option<{}>", id_type)
            } else {
                id_type.to_string()
            };

            let struct_name = table.name.to_case(Case::Pascal);

            generated.line("#[derive(Debug, Clone, Copy)]");
            generated.block(f!("pub enum {}Column {{", struct_name), |b| {
                for col in &table.cols {
                    let variant_name = col.field_name.to_case(Case::Pascal);
                    b.line(f!("{},", variant_name));
                }
            });
            
            generated.block(f!("impl {}Column {{", struct_name), |b| {
                b.block("pub fn as_str(&self) -> &'static str {", |b| {
                    b.line("match self {");
                    for col in &table.cols {
                        let variant = col.field_name.to_case(Case::Pascal);
                        b.line(f!("    Self::{} => \"{}\",", variant, col.name));
                    }
                    b.line("}");
                });
                
                b.block("pub fn qualified(&self, table: &str) -> String {", |b| {
                    b.line("format!(\"{}.{}\", table, self.as_str())");
                });
                
                b.block("pub fn of_table(&self) -> String {", |b| {
                    b.line(f!("format!(\"{}.{{}}\", self.as_str())", table.name));
                });

                b.block("pub fn of(self) -> ColumnRef {", |b| {
                    b.line("ColumnRef {");
                    b.line(f!("    table: \"{}\".to_string(),", table.name));
                    b.line("    column: self.as_str().to_string(),");
                    b.line("}");
                });

            });
            
            let table_struct_name = format!("{}", struct_name);
            let query_struct_name = format!("{}Query", struct_name);
            
            generated.line("#[derive(Debug, Clone, Serialize, FromRow)]");

            generated.block(f!("pub struct {} {{", table_struct_name), |b| {
                for col in &table.cols {
                    let rust_type = col.rust_type();

                    b.line(f!("#[sqlx(rename = \"{}\")]", col.name));

                    if col.name == "id" {
                        b.line(f!("pub id: {},", id_type));
                    } else {
                        if col.not_null == 0 {
                            b.line(f!("pub {}: Option<{}>,", col.field_name, rust_type));
                        } else {
                            b.line(f!("pub {}: {},", col.field_name, rust_type));
                        }
                    }
                }
            });
            
            generated.line("#[derive(Debug, Clone, Default)]");

            generated.block(f!("pub struct {} {{", query_struct_name), |b| {
                for col in &table.cols {
                    let rust_type = col.cow_rust_type();

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

            // struct impl
            generated.block(f!("impl {} {{", table_struct_name), |b| {
                b.line(f!("pub const NAME: &'static str = \"{}\";", table.name));

                for col in &table.cols {
                    let const_name = col.name.to_case(Case::UpperSnake);
                    let variant = col.field_name.to_case(Case::Pascal);
                    b.line(f!("pub const {}: {}Column = {}Column::{};", const_name, struct_name, struct_name, variant));
                }

                // INSERT
                b.block(f!("pub async fn insert(&self, pool: &sqlx::{}) -> Result<(), sqlx::Error> {{", self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!(
                        "let mut q = sqlx::query(\"INSERT INTO {} ({}) VALUES ({})\");",
                        table.name, field_list, placeholders
                    ));

                    for f in &rust_field_names {
                        b.line(f!("q = q.bind(&self.{});", f));
                    }

                    b.line("let result = q.execute(pool).await?;");
                    b.line("Ok(())");
                });

                // UPDATE
                b.block(f!("pub async fn update(&self, pool: &sqlx::{}) -> Result<(), sqlx::Error> {{", self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!(
                        "let mut q = sqlx::query(\"UPDATE {} SET {} WHERE id = {}\");",
                        table.name, update_fields, self.pool.type_mapper().placeholder(field_names.len())
                    ));

                    for f in &rust_field_names {
                        b.line(f!("q = q.bind(&self.{});", f));
                    }

                    b.line("q = q.bind(&self.id);");
                    b.line("q.execute(pool).await?;");
                    b.line("Ok(())");
                });
               
                // DELETE
                b.block(f!("pub async fn delete(id: {}, pool: &sqlx::{}) -> Result<(), sqlx::Error> {{", id_type, self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!("sqlx::query(\"DELETE FROM {} WHERE id = {}\")", table.name, self.pool.type_mapper().placeholder(0)));
                    b.line("    .bind(id)");
                    b.line("    .execute(pool)");
                    b.line("    .await?;");
                    b.line("Ok(())");
                });
                
                // GET BY ID
                b.block(f!("pub async fn get_by_id(id: {}, pool: &sqlx::{}) -> Result<Option<Self>, sqlx::Error> {{", id_type, self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!(
                        "let row = sqlx::query_as::<_, Self>(\"SELECT {} FROM {} WHERE id = {}\")",
                        select_fields, table.name, self.pool.type_mapper().placeholder(0)
                    ));
                    b.line("    .bind(id)");
                    b.line("    .fetch_optional(pool)");
                    b.line("    .await?;");
                    b.line("Ok(row)");
                });

                // GET ALL
                b.block(f!("pub async fn get_all(pool: &sqlx::{}) -> Result<Vec<Self>, sqlx::Error> {{", self.pool.type_mapper().sqlx_type()), |b| {
                    b.line(f!("let rows = sqlx::query_as::<_, Self>(\"SELECT {} FROM {}\")", select_fields, table.name));
                    b.line("    .fetch_all(pool)");
                    b.line("    .await?;");
                    b.line("Ok(rows)");
                });

                // QUERY BUILDER METHOD
                b.block(f!("pub fn query() -> {} {{", query_struct_name), |b| {
                    b.line(f!("{}::default()", query_struct_name));
                });
            });

            generated.block(f!("impl {} {{", query_struct_name), |b| {
                for col in &table.cols {
                    // let col_name: String = col.value("name").;
                    // let col_type: String = col.get("type");
                    // let rust_field = col_name.to_case(Case::Snake);
                    
                    let (rust_type, is_cow) = col.cow_rust_type();

                    if is_cow {
                        b.block(f!("pub fn {}<'a>(mut self, value: impl Into<{}>) -> Self {{", col.field_name, rust_type), |b| {
                            b.line(f!("self.{} = Some(value.into());", col.field_name));
                            b.line("self");
                        });
                    } else {
                        b.block(f!("pub fn {}(mut self, value: {}) -> Self {{", col.field_name, rust_type), |b| {
                            b.line(f!("self.{} = Some(value);", col.field_name));
                            b.line("self");
                        });
                    }
                    
                    if col.is_numeric() {
                        let base_type = col.kind.rust_type();
                        
                        b.block(f!("pub fn {}_gt(mut self, value: {}) -> Self {{", col.field_name, base_type), |b| {
                            b.line(f!("self.{}_gt = Some(value);", col.field_name));
                            b.line("self");
                        });
                        

                        b.block(f!("pub fn {}_gte(mut self, value: {}) -> Self {{", col.field_name, base_type), |b| {
                            b.line(f!("self.{}_gte = Some(value);", col.field_name));
                            b.line("self");
                        });

                        b.block(f!("pub fn {}_lt(mut self, value: {}) -> Self {{", col.field_name, base_type), |b| {
                            b.line(f!("self.{}_lt = Some(value);", col.field_name));
                            b.line("self");
                        });

                        b.block(f!("pub fn {}_lte(mut self, value: {}) -> Self {{", col.field_name, base_type), |b| {
                            b.line(f!("self.{}_lte = Some(value);", col.field_name));
                            b.line("self");
                        });
                        
                        b.block(f!("pub fn {}_between(mut self, start: {}, end: {}) -> Self {{", col.field_name, base_type, base_type), |b| {
                            b.line(f!("self.{}_gte = Some(start);", col.field_name));
                            b.line(f!("self.{}_lte = Some(end);", col.field_name));
                            b.line("self");
                        });
                    }
                }

                for fk in &table.fks {
                    let ref_struct = fk.ref_table.to_case(Case::Pascal);
                    let join_method = format!("join_{}", fk.ref_table.to_case(Case::Snake));
                    let left_join_method = format!("left_join_{}", fk.ref_table.to_case(Case::Snake));
                    let right_join_method = format!("right_join_{}", fk.ref_table.to_case(Case::Snake));
                    
                    b.line(f!("/// Type-safe inner join to {} on {}.{} = {}.{}", 
                              fk.ref_table, table.name, fk.column, fk.ref_table, fk.ref_column));
                    b.block(f!("pub fn {}(mut self) -> Self {{", join_method), |b| {
                        b.line("self.joins.push(Join {");
                        b.line("    join_type: JoinType::Inner,");
                        b.line(f!("    table: \"{}\".to_string(),", fk.ref_table));
                        b.line(f!("    on_condition: format!(\"{}.{{}} = {}.{{}}\", ", table.name, fk.ref_table));
                        b.line(f!("        {}Column::{}.as_str(), ", 
                                  struct_name, 
                                  fk.column.to_case(Case::Pascal)));
                        b.line(f!("        {}Column::{}.as_str()),", 
                                  ref_struct,
                                  fk.ref_column.to_case(Case::Pascal)));
                        b.line("});");
                        b.line("self");
                    });
                    
                    b.line(f!("/// Type-safe left join to {} on {}.{} = {}.{}", 
                              fk.ref_table, table.name, fk.column, fk.ref_table, fk.ref_column));
                    b.block(f!("pub fn {}(mut self) -> Self {{", left_join_method), |b| {
                        b.line("self.joins.push(Join {");
                        b.line("    join_type: JoinType::Left,");
                        b.line(f!("    table: \"{}\".to_string(),", fk.ref_table));
                        b.line(f!("    on_condition: format!(\"{}.{{}} = {}.{{}}\", ", table.name, fk.ref_table));
                        b.line(f!("        {}Column::{}.as_str(), ", 
                                  struct_name, 
                                  fk.column.to_case(Case::Pascal)));
                        b.line(f!("        {}Column::{}.as_str()),", 
                                  ref_struct,
                                  fk.ref_column.to_case(Case::Pascal)));
                        b.line("});");
                        b.line("self");
                    });
                    
                    b.line(f!("/// Type-safe right join to {} on {}.{} = {}.{}", 
                              fk.ref_table, table.name, fk.column, fk.ref_table, fk.ref_column));
                    b.block(f!("pub fn {}(mut self) -> Self {{", right_join_method), |b| {
                        b.line("self.joins.push(Join {");
                        b.line("    join_type: JoinType::Right,");
                        b.line(f!("    table: \"{}\".to_string(),", fk.ref_table));
                        b.line(f!("    on_condition: format!(\"{}.{{}} = {}.{{}}\", ", table.name, fk.ref_table));
                        b.line(f!("        {}Column::{}.as_str(), ", 
                                  struct_name, 
                                  fk.column.to_case(Case::Pascal)));
                        b.line(f!("        {}Column::{}.as_str()),", 
                                  ref_struct,
                                  fk.ref_column.to_case(Case::Pascal)));
                        b.line("});");
                        b.line("self");
                    });
                }

                b.block("pub fn join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self {", |b| {
                    b.line("self.joins.push(Join {");
                    b.line("    join_type: JoinType::Inner,");
                    b.line("    table: table.into(),");
                    b.line("    on_condition: left.eq(&right),");
                    b.line("});");
                    b.line("self");
                });
                
                b.block("pub fn left_join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self {", |b| {
                    b.line("self.joins.push(Join {");
                    b.line("    join_type: JoinType::Left,");
                    b.line("    table: table.into(),");
                    b.line("    on_condition: left.eq(&right),");
                    b.line("});");
                    b.line("self");
                });
                
                b.block("pub fn right_join(mut self, table: impl Into<String>, left: ColumnRef, right: ColumnRef) -> Self {", |b| {
                    b.line("self.joins.push(Join {");
                    b.line("    join_type: JoinType::Right,");
                    b.line("    table: table.into(),");
                    b.line("    on_condition: left.eq(&right),");
                    b.line("});");
                    b.line("self");
                });

                b.block("pub fn where(mut self, condition: String) -> Self {", |b| {
                    b.line("self.where_clauses.push(condition);");
                    b.line("self");
                });

                b.block("pub fn limit(mut self, value: i32) -> Self {", |b| {
                    b.line("self.limit = Some(value);");
                    b.line("self");
                });

                b.block("pub fn offset(mut self, value: i32) -> Self {", |b| {
                    b.line("self.offset = Some(value);");
                    b.line("self");
                });

                b.block(f!("pub async fn fetch_all(self, pool: &sqlx::{}) -> Result<Vec<{}>, sqlx::Error> {{", self.pool.type_mapper().sqlx_type(), struct_name), |b| {
                    b.line("let mut conditions = Vec::new();");

                    if self.pool.type_mapper().uses_numbered_placeholders() {
                        b.line("let mut bind_idx = 0;");
                    }

                    // build condition checks
                    for col in &table.cols {
                        if self.pool.type_mapper().uses_numbered_placeholders() {
                            b.block(f!("if self.{}.is_some() {{", col.field_name), |b| {
                                b.line("bind_idx += 1;");
                                b.line(f!("conditions.push(format!(\"{}.{} = ${{}}\", bind_idx));", table.name, col.name));
                            });

                            if col.is_numeric() {
                                b.block(f!("if self.{}_gt.is_some() {{", col.field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} > ${{}}\", bind_idx));", col.name));
                                });
                                b.block(f!("if self.{}_gte.is_some() {{", col.field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} >= ${{}}\", bind_idx));", col.name));
                                });
                                b.block(f!("if self.{}_lt.is_some() {{", col.field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} < ${{}}\", bind_idx));", col.name));
                                });
                                b.block(f!("if self.{}_lte.is_some() {{", col.field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} <= ${{}}\", bind_idx));", col.name));
                                });
                            }
                        } else {
                            b.block(f!("if self.{}.is_some() {{", col.field_name), |b| {
                                b.line(f!("conditions.push(\"{}.{} = ?\".to_string());", table.name, col.name));
                            });

                            if col.is_numeric() {
                                b.block(f!("if self.{}_gt.is_some() {{", col.field_name), |b| {
                                    b.line(f!("conditions.push(\"{} > ?\".to_string());", col.name));
                                });
                                b.block(f!("if self.{}_gte.is_some() {{", col.field_name), |b| {
                                    b.line(f!("conditions.push(\"{} >= ?\".to_string());", col.name));
                                });
                                b.block(f!("if self.{}_lt.is_some() {{", col.field_name), |b| {
                                    b.line(f!("conditions.push(\"{} < ?\".to_string());", col.name));
                                });
                                b.block(f!("if self.{}_lte.is_some() {{", col.field_name), |b| {
                                    b.line(f!("conditions.push(\"{} <= ?\".to_string());", col.name));
                                });
                            }
                        }
                    }

                    b.block("for clause in &self.where_clauses {", |b| {
                        b.line("conditions.push(clause.clone());");
                    });

                    let mut sql_string = String::from("let mut sql = format!(\"SELECT {} FROM {}\", vec![");

                    for (i, col) in table.cols.iter().enumerate() {
                        if i > 0 {
                            sql_string.push_str(", ");
                        }
                        sql_string.push_str(&format!("\"{}\"", col.name));
                    }

                    sql_string.push_str(&format!("].join(\", \"), \"{}\");", table.name));

                    b.line(sql_string);

                    b.block("if !conditions.is_empty() {", |b| {
                        b.line("sql.push_str(\" WHERE \");");
                        b.line("sql.push_str(&conditions.join(\" AND \"));");
                    });

                    b.block("for join in &self.joins {", |b| {
                        b.line("let join_type_str = match join.join_type {");
                        b.line("    JoinType::Inner => \"INNER JOIN\",");
                        b.line("    JoinType::Left => \"LEFT JOIN\",");
                        b.line("    JoinType::Right => \"RIGHT JOIN\",");
                        b.line("};");
                        b.line("sql.push_str(&format!(\" {} {} ON {}\", join_type_str, join.table, join.on_condition));");
                    });

                    b.block("if let Some(lim) = self.limit {", |b| {
                        b.line("sql.push_str(&format!(\" LIMIT {}\", lim));");
                    });
                    
                    b.block("if let Some(off) = self.offset {", |b| {
                        b.line("sql.push_str(&format!(\" OFFSET {}\", off));");
                    });
                    
                    b.line("let mut query = sqlx::query_as(&sql);");

                    for col in &table.cols {
                        b.block(f!("if let Some(ref val) = self.{} {{", col.field_name), |b| {
                            if col.is_string_like() {
                                if col.kind == DbType::Blob {
                                    b.line("query = query.bind::<&[u8]>(val.as_ref());");
                                } else {
                                    b.line("query = query.bind(val.as_ref());");
                                }
                            } else {
                                b.line("query = query.bind(val);");
                            }
                        });

                        if col.is_numeric() {
                            b.block(f!("if let Some(val) = self.{}_gt {{", col.field_name), |b| {
                                b.line("query = query.bind(val);");
                            });
                            b.block(f!("if let Some(val) = self.{}_gte {{", col.field_name), |b| {
                                b.line("query = query.bind(val);");
                            });
                            b.block(f!("if let Some(val) = self.{}_lt {{", col.field_name), |b| {
                                b.line("query = query.bind(val);");
                            });
                            b.block(f!("if let Some(val) = self.{}_lte {{", col.field_name), |b| {
                                b.line("query = query.bind(val);");
                            });
                        }
                    }
                    
                    b.line("query.fetch_all(pool).await");
                });

                b.block(f!("pub async fn fetch_one(self, pool: &sqlx::{}) -> Result<Option<{}>, sqlx::Error> {{", self.pool.type_mapper().sqlx_type(), struct_name), |b| {
                    b.line("let mut results = self.limit(1).fetch_all(pool).await?;");
                    b.line("Ok(results.pop())");
                });
            });
        }

        // println!("{}", generated.buf);

        let out_dir = env::var("OUT_DIR")
            .wrap(|| "failed to get out_dir at compile time")?;
        let dest_path = Path::new(&out_dir).join("orm.rs");
        fs::write(&dest_path, generated.buf)?;

        Ok(())
    }
}

/// debug tests --- just for now
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

            let orm = Codegen::new(pool.clone(), MIGR_DIR)
                .await
                .encase(|| "failed to make new orm")?;

            orm.run_codegen().await?;
           
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

            let orm = Codegen::new(pool.clone(), MIGR_DIR)
                .await
                .encase(|| "failed to make new orm")?;

            orm.run_codegen().await?;
           
            Ok::<(), luhtwin::AnyError>(())
        }).encase(|| "failed to do runtime db work")?;

        Ok(())
    }

    #[test]
    fn test_string_view_indentation() {
        let mut view = RustStringView::new();
        
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
