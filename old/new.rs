use std::{env, fs};
use std::borrow::Cow;
use std::path::{Path, PathBuf};

use std::collections::HashMap;
use serde_json::Value;

use convert_case::{Casing, Case};
use luhtwin::{at, Encase, LuhTwin, Wrap};
use sha2::{Digest, Sha256};
use sqlx::{Column, Pool, Postgres, Row, Sqlite};

/// A simple but robust code-generation builder.
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

    pub fn from(cols: HashMap<String, Value>) -> Self {
        Self { cols }
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

pub trait TryCollect<T> {
    fn try_collect(self) -> LuhTwin<Vec<T>>;
}

impl<I, T> TryCollect<T> for I
where
    I: Iterator<Item = LuhTwin<T>>,
{
    fn try_collect(self) -> LuhTwin<Vec<T>> {
        self.collect()
    }
}

pub enum LuhValue<'a> {
    I64(i64),
    F64(f64),
    String(&'a str),
    Bool(bool),
}

pub struct ColumnData {
    name: String,
    kind: String,
    field_name: String,
    not_null: i64,
}

impl ColumnData {
    pub fn from_sqlite_rows<'a>(
        rows: impl IntoIterator<Item = &'a LuhRow>,
    ) -> LuhTwin<Vec<ColumnData>> {
        rows.into_iter()
            .map(|c| {
                let name = c.value("name")
                    .string()
                    .require()
                    .wrap(|| "column 'name' must be a string")?;

                Ok(ColumnData {
                    field_name: name.to_case(Case::Snake),
                    name,
                    kind: c.value("type")
                        .string()
                        .require()
                        .wrap(|| "column 'type' must be a string")?,
                    not_null: c.value("notnull")
                        .i64()
                        .require()
                        .wrap(|| "column 'notnull' must be an integer")?,
                })
            })
            .collect()
    }

    pub fn from_pq_rows<'a>(
        rows: impl IntoIterator<Item = &'a LuhRow>,
    ) -> LuhTwin<Vec<ColumnData>> {
        rows.into_iter()
            .map(|c| {
                let name = c.value("column_name").string().require()
                    .wrap(|| "column 'name' must be a string")?;
                let kind = c.value("data_type").string().require()
                    .wrap(|| "column 'type' must be a string")?;
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
}

pub struct TableData {
    pub cols: Vec<ColumnData>,
    pub name: String
}

pub trait ColumnTypeMapper {
    fn rust_type(&self, col: &ColumnData) -> String;
    fn cow_rust_type(&self, col: &ColumnData) -> (String, bool);
}

pub struct SqliteMapper;

impl ColumnTypeMapper for SqliteMapper {
    fn rust_type(&self, col: &ColumnData) -> String {
        let base = match col.kind.to_uppercase().as_str() {
            "INTEGER" | "BIGINT" => "i64",
            "TEXT" | "TIMESTAMP" => "String",
            "BLOB" => "Vec<u8>",
            "REAL" => "f64",
            "BOOLEAN" => "bool",
            _ => "String",
        };

        if col.not_null == 0 {
            format!("Option<{}>", base)
        } else {
            base.to_string()
        }
    }

    fn cow_rust_type(&self, col: &ColumnData) -> (String, bool) {
        match col.kind.to_uppercase().as_str() {
            "TEXT" | "BLOB" | "TIMESTAMP" => ("Cow<'static, str>".to_string(), true),
            _ => (self.rust_type(col), false),
        }
    }
}

pub struct PostgresMapper;

impl ColumnTypeMapper for PostgresMapper {
    fn rust_type(&self, col: &ColumnData) -> String {
        match col.kind.to_lowercase().as_str() {
            "bigint" | "int8" => "i64".to_string(),
            "integer" | "int4" => "i32".to_string(),
            "text" | "varchar" | "char" => "String".to_string(),
            "boolean" | "bool" => "bool".to_string(),
            "bytea" => "Vec<u8>".to_string(),
            "numeric" | "decimal" => "f64".to_string(),
            "timestamp" | "timestamptz" => "String".to_string(),
            _ => "String".to_string(),
        }
    }

    fn cow_rust_type(&self, col: &ColumnData) -> (String, bool) {
        match col.kind.to_lowercase().as_str() {
            "text" | "varchar" | "char" | "bytea" | "timestamp" | "timestamptz" => ("Cow<'static, str>".to_string(), true),
            _ => (self.rust_type(col), false),
        }
    }
}

pub enum Backend {
    Postgres,
    Sqlite
}

impl Backend {
    fn placeholder(&self, idx: usize) -> String {
        match self {
            Backend::Sqlite => "?".into(),
            Backend::Postgres => format!("${}", idx + 1),
        }
    }
}

#[async_trait::async_trait]
pub trait LuhOrm: Send + Sync {
    fn type_mapper(&self) -> Box<dyn ColumnTypeMapper>;
    fn kind(&self) -> Backend;

    async fn execute_raw(&self, query: &str) -> LuhTwin<u64>;
    async fn fetch_all_raw(&self, query: &str) -> LuhTwin<Vec<LuhRow>>;

    async fn get_master_table(&self) -> LuhTwin<Vec<TableData>>;
    async fn execute_with_params(&self, query: &str, params: &[LuhValue]) -> LuhTwin<u64>;
}

#[async_trait::async_trait]
impl LuhOrm for Pool<Postgres> {
    fn type_mapper(&self) -> Box<dyn ColumnTypeMapper> {
        Box::new(PostgresMapper)
    }

    fn kind(&self) -> Backend {
        Backend::Postgres
    }

    async fn execute_raw(&self, query: &str) -> LuhTwin<u64> {
        let res = sqlx::query(query)
            .execute(self)
            .await
            .wrap(|| format!("failed to execute query: {}", query))?;
        Ok(res.rows_affected())
    }

    async fn fetch_all_raw(&self, query: &str) -> LuhTwin<Vec<LuhRow>> {
        let rows = sqlx::query(query)
            .fetch_all(self)
            .await
            .wrap(|| format!("failed to fetch_all query: {}", query))?;

        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let mut map = std::collections::HashMap::new();

            for col in row.columns() {
                let col_name = col.name();

                let val: Value = if let Ok(s) = row.try_get::<String, _>(col_name) {
                    Value::String(s)
                } else if let Ok(i) = row.try_get::<i64, _>(col_name) {
                    Value::Number(i.into())
                } else if let Ok(f) = row.try_get::<f64, _>(col_name) {
                    Value::Number(serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)))
                } else if let Ok(b) = row.try_get::<bool, _>(col_name) {
                    Value::Bool(b)
                } else if let Ok(dt) = row.try_get::<chrono::NaiveDateTime, _>(col_name) {
                    Value::String(dt.to_string())
                } else if let Ok(date) = row.try_get::<chrono::NaiveDate, _>(col_name) {
                    Value::String(date.to_string())
                } else if let Ok(bytes) = row.try_get::<Vec<u8>, _>(col_name) {
                    Value::Array(bytes.iter().map(|b| Value::Number((*b).into())).collect())
                } else {
                    Value::Null
                };

                map.insert(col_name.to_string(), val);
            }

            results.push(LuhRow::from(map));
        }

        Ok(results)
    }

    async fn get_master_table(&self) -> LuhTwin<Vec<TableData>> {
        let rows = self.fetch_all_raw(
            "SELECT table_name
             FROM information_schema.tables
             WHERE table_schema = 'public'
             AND table_type = 'BASE TABLE';"
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
            )).await.wrap(|| format!("failed to get columns for table \"{}\"", table))?;
            
            tables.push(TableData {
                cols: ColumnData::from_pq_rows(&cols)?,
                name: table,
            });
        }
        
        Ok(tables)
    }

    async fn execute_with_params(&self, query: &str, params: &[LuhValue]) -> LuhTwin<u64> {
        let mut q = sqlx::query(query);

        for p in params {
            match p {
                LuhValue::I64(v) => q = q.bind(v),
                LuhValue::F64(v) => q = q.bind(v),
                LuhValue::String(v) => q = q.bind(*v),
                LuhValue::Bool(v) => q = q.bind(v),
            }
        }

        let res = q.execute(self)
            .await
            .wrap(|| format!("failed to execute query: {}", query))?;

        Ok(res.rows_affected())
    }
}

#[async_trait::async_trait]
impl LuhOrm for Pool<Sqlite> {
    fn type_mapper(&self) -> Box<dyn ColumnTypeMapper> {
        Box::new(SqliteMapper)
    }

    fn kind(&self) -> Backend {
        Backend::Sqlite
    }

    async fn execute_raw(&self, query: &str) -> LuhTwin<u64> {
        let res = sqlx::query(query)
            .execute(self)
            .await
            .wrap(|| format!("failed to execute query: {}", query))?;
        Ok(res.rows_affected())
    }

    async fn fetch_all_raw(&self, query: &str) -> LuhTwin<Vec<LuhRow>> {
        let rows = sqlx::query(query)
            .fetch_all(self)
            .await
            .wrap(|| format!("failed to fetch_all query: {}", query))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let mut map = std::collections::HashMap::new();
            for col in row.columns() {
                let val: Value = if let Ok(s) = row.try_get::<String, _>(col.name()) {
                    serde_json::from_str(&s).unwrap_or(Value::String(s))
                } else if let Ok(i) = row.try_get::<i64, _>(col.name()) {
                    Value::Number(i.into())
                } else if let Ok(f) = row.try_get::<f64, _>(col.name()) {
                    Value::Number(serde_json::Number::from_f64(f).unwrap())
                } else if let Ok(b) = row.try_get::<bool, _>(col.name()) {
                    Value::Bool(b)
                } else {
                    Value::Null
                };
                map.insert(col.name().to_string(), val);
            }
            results.push(map);
        }
        Ok(results
           .into_iter()
           .map(|r| LuhRow::from(r))
           .collect())
    }

    async fn get_master_table(&self) -> LuhTwin<Vec<TableData>> {
        let rows = self.fetch_all_raw("SELECT name FROM sqlite_master WHERE type='table'")
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
            let cols = self.fetch_all_raw(&format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\"")))
                .await
                .wrap(|| format!("failed to get columns for table \"{}\"", table))?;

            tables.push(TableData {
                cols: ColumnData::from_sqlite_rows(&cols)?,
                name: table
            });
        };

        Ok(tables)
    }

    async fn execute_with_params(&self, query: &str, params: &[LuhValue]) -> LuhTwin<u64> {
        let mut q = sqlx::query(query);

        for p in params {
            match p {
                LuhValue::I64(v) => q = q.bind(v),
                LuhValue::F64(v) => q = q.bind(v),
                LuhValue::String(v) => q = q.bind(*v),
                LuhValue::Bool(v) => q = q.bind(v),
            }
        }

        let res = q.execute(self)
            .await
            .wrap(|| format!("failed to execute query: {}", query))?;

        Ok(res.rows_affected())
    }
}


pub async fn migrate<D: LuhOrm>(pool: &D, migr_dir: impl Into<PathBuf>) -> LuhTwin<()> {
    let migr_dir: PathBuf = migr_dir.into();

    pool.execute_raw(
        "CREATE TABLE IF NOT EXISTS _luhorm_migrations (
             id SERIAL PRIMARY KEY,
             filename TEXT NOT NULL UNIQUE,
             hash TEXT NOT NULL,
             applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
         );"
    ).await.wrap(|| "failed to run migrations table schema sql")?;

    let applied: Vec<(String, String)> = pool
        .fetch_all_raw("SELECT filename, hash FROM _luhorm_migrations ORDER BY id")
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
    
    // alphanumerical order
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
        
        pool.execute_raw(&sql_str)
            .await
            .wrap(|| format!("failed to execute migration: {}", filename))?;

        let insert_sql = match pool.kind() {
            Backend::Postgres => {
                "INSERT INTO _luhorm_migrations (filename, hash) VALUES ($1, $2)"
            },
            Backend::Sqlite => {
                "INSERT INTO _luhorm_migrations (filename, hash) VALUES (?, ?)"
            }
        };

        pool.execute_with_params(
            &insert_sql,
            &[
                LuhValue::String(&filename),
                LuhValue::String(&file_hash)
            ]
        ).await.wrap(|| "failed to insert new migration to table")?;

        println!("applied migration: {}", filename);
    }

    Ok(())
}

macro_rules! f {
    ($($args:tt)*) => {
        Cow::Owned(format!($($args)*))
    };
}

pub struct Codegen {
    pool: Box<dyn LuhOrm>,
}

impl Codegen {
    pub async fn new_sqlite(pool: Pool<Sqlite>, migr_dir: impl Into<PathBuf>) -> LuhTwin<Self> {
        let migr_dir: PathBuf = migr_dir.into();

        if !migr_dir.exists() && !migr_dir.is_dir() {
            return Err(at!("migr dir doesn't exist or is a directory: {}", migr_dir.display()).into())
        }

        migrate(&pool, migr_dir)
            .await
            .encase(|| "failed to migrate when making new LuhOrm")?;

        let x = Self {
            pool: Box::new(pool),
        };

        Ok(x)
    }

    pub async fn new_postgres(pool: Pool<Postgres>, migr_dir: impl Into<PathBuf>) -> LuhTwin<Self> {
        let migr_dir: PathBuf = migr_dir.into();

        if !migr_dir.exists() && !migr_dir.is_dir() {
            return Err(at!("migr dir doesn't exist or is a directory: {}", migr_dir.display()).into())
        }

        migrate(&pool, migr_dir)
            .await
            .encase(|| "failed to migrate when making new LuhOrm")?;

        let x = Self {
            pool: Box::new(pool),
        };

        Ok(x)
    }

    pub async fn run_codegen(&self) -> LuhTwin<()> {
        let tables = self.pool.get_master_table()
            .await
            .wrap(|| "failed to get master table in codegen")?;

        let mut generated = RustStringView::new();

        generated.line("use serde::Serialize;");
        generated.line("use sqlx::FromRow;");
        generated.line("use std::borrow::Cow;");
        
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
                .map(|i| self.pool.kind().placeholder(i))
                .collect::<Vec<_>>()
                .join(", ");

            let select_fields = table.cols.iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(", ");

            let update_fields = field_names
                .iter()
                .enumerate()
                .map(|(i, f)| format!("{} = {}", f, self.pool.kind().placeholder(i)))
                .collect::<Vec<_>>()
                .join(", ");

            let mapper = self.pool.type_mapper();

            let struct_name = table.name.to_case(Case::Pascal);
            
            generated.line("#[derive(Debug, Clone, Serialize, FromRow)]");

            generated.block(f!("pub struct {} {{", struct_name), |b| {
                for col in &table.cols {
                    let rust_type = mapper.rust_type(&col);
                    
                    b.line(f!("#[sqlx(rename = \"{}\")]", col.name));
                    b.line(f!("pub {}: {},", col.field_name, rust_type));
                }
            });
            
            let query_struct_name = format!("{}Query", struct_name);

            generated.line("#[derive(Debug, Clone, Default)]");

            generated.block(f!("pub struct {} {{", query_struct_name), |b| {
                for col in &table.cols {
                    let rust_type = mapper.cow_rust_type(&col);

                    let name = col.name.to_case(Case::Snake);

                    b.line(f!("pub {}: Option<{}>,", name, rust_type.0));
                    
                    if matches!(col.kind.to_uppercase().as_str(), "INTEGER" | "BIGINT" | "REAL") {
                        let base_type = match col.kind.to_uppercase().as_str() {
                            "INTEGER" => "i64",
                            "BIGINT" => "i64",
                            "REAL" => "f64",
                            _ => "i64",
                        };
                        b.line(f!("pub {}_gt: Option<{}>,", name, base_type));
                        b.line(f!("pub {}_gte: Option<{}>,", name, base_type));
                        b.line(f!("pub {}_lt: Option<{}>,", name, base_type));
                        b.line(f!("pub {}_lte: Option<{}>,", name, base_type));
                    }
                }
                
                b.line("pub limit: Option<i32>,");
                b.line("pub offset: Option<i32>,");
            });

            // struct impl
            generated.block(f!("impl {} {{", struct_name), |b| {
                // INSERT
                b.block("pub async fn insert(&self, pool: &sqlx::SqlitePool) -> Result<i64, sqlx::Error> {", |b| {
                    b.line(f!(
                        "let mut q = sqlx::query(\"INSERT INTO {} ({}) VALUES ({})\");",
                        table.name, field_list, placeholders
                    ));

                    for f in &rust_field_names {
                        b.line(f!("q = q.bind(&self.{});", f));
                    }

                    b.line("let result = q.execute(pool).await?;");
                    b.line("Ok(result.last_insert_rowid())");
                });

                // UPDATE
                b.block("pub async fn update(&self, pool: &sqlx::SqlitePool) -> Result<(), sqlx::Error> {", |b| {
                    b.line(f!(
                        "let mut q = sqlx::query(\"UPDATE {} SET {} WHERE id = {}\");",
                        table.name, update_fields, self.pool.kind().placeholder(field_names.len())
                    ));

                    for f in &rust_field_names {
                        b.line(f!("q = q.bind(&self.{});", f));
                    }

                    b.line("q = q.bind(&self.id);");
                    b.line("q.execute(pool).await?;");
                    b.line("Ok(())");
                });
               
                // DELETE
                b.block("pub async fn delete(id: i32, pool: &sqlx::SqlitePool) -> Result<(), sqlx::Error> {", |b| {
                    b.line(f!("sqlx::query(\"DELETE FROM {} WHERE id = {}\")", table.name, self.pool.kind().placeholder(0)));
                    b.line("    .bind(id)");
                    b.line("    .execute(pool)");
                    b.line("    .await?;");
                    b.line("Ok(())");
                });
                
                // GET BY ID
                b.block("pub async fn get_by_id(id: i32, pool: &sqlx::SqlitePool) -> Result<Option<Self>, sqlx::Error> {", |b| {
                    b.line(f!(
                        "let row = sqlx::query_as::<_, Self>(\"SELECT {} FROM {} WHERE id = {}\")",
                        select_fields, table.name, self.pool.kind().placeholder(0)
                    ));
                    b.line("    .bind(id)");
                    b.line("    .fetch_optional(pool)");
                    b.line("    .await?;");
                    b.line("Ok(row)");
                });

                // GET ALL
                b.block("pub async fn get_all(pool: &sqlx::SqlitePool) -> Result<Vec<Self>, sqlx::Error> {", |b| {
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
                    
                    let (rust_type, is_cow) = mapper.cow_rust_type(&col);

                    if is_cow {
                        b.block(f!("pub fn {}(mut self, value: impl Into<{}>) -> Self {{", col.field_name, rust_type), |b| {
                            b.line(f!("self.{} = Some(value.into());", col.field_name));
                            b.line("self");
                        });
                    } else {
                        b.block(f!("pub fn {}(mut self, value: {}) -> Self {{", col.field_name, rust_type), |b| {
                            b.line(f!("self.{} = Some(value);", col.field_name));
                            b.line("self");
                        });
                    }
                    
                    if matches!(col.kind.to_uppercase().as_str(), "INTEGER" | "BIGINT" | "REAL") {
                        let base_type = match col.kind.to_uppercase().as_str() {
                            "INTEGER" => "i64",
                            "BIGINT" => "i64", 
                            "REAL" => "f64",
                            _ => "i64",
                        };
                        
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

                b.block("pub fn limit(mut self, value: i32) -> Self {", |b| {
                    b.line("self.limit = Some(value);");
                    b.line("self");
                });

                b.block("pub fn offset(mut self, value: i32) -> Self {", |b| {
                    b.line("self.offset = Some(value);");
                    b.line("self");
                });

                b.block(f!("pub async fn fetch_all(self, pool: &sqlx::SqlitePool) -> Result<Vec<{}>, sqlx::Error> {{", struct_name), |b| {
                    b.line("let mut conditions = Vec::new();");

                    match self.pool.kind() {
                        Backend::Postgres => {
                            b.line("let mut bind_idx = 0;");
                        }
                        _ => {}
                    }

                    // build condition checks
                    for col in &table.cols {
                        match self.pool.kind() {
                            Backend::Postgres => {
                                b.block(f!("if self.{}.is_some() {{", col.field_name), |b| {
                                    b.line("bind_idx += 1;");
                                    b.line(f!("conditions.push(format!(\"{} = ${{}}\", bind_idx));", col.name));
                                });
                            },
                            Backend::Sqlite => {
                                b.block(f!("if self.{}.is_some() {{", col.field_name), |b| {
                                    b.line(f!("conditions.push(\"{} = ?\");", col.name));
                                });
                            }
                        }
                    }

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

                    b.block("if let Some(lim) = self.limit {", |b| {
                        b.line("sql.push_str(&format!(\" LIMIT {}\", lim));");
                    });
                    
                    b.block("if let Some(off) = self.offset {", |b| {
                        b.line("sql.push_str(&format!(\" OFFSET {}\", off));");
                    });
                    
                    b.line("let mut query = sqlx::query_as(&sql);");

                    for col in &table.cols {
                        let needs_as_ref = matches!(col.kind.to_uppercase().as_str(), "TEXT" | "BLOB" | "TIMESTAMP");
                        
                        b.block(f!("if let Some(ref val) = self.{} {{", col.field_name), |b| {
                            if needs_as_ref {
                                b.line("query = query.bind(val.as_ref());");
                            } else {
                                b.line("query = query.bind(val);");
                            }
                        });
                    }
                    
                    b.line("query.fetch_all(pool).await");
                });

                b.block(f!("pub async fn fetch_one(self, pool: &sqlx::SqlitePool) -> Result<Option<{}>, sqlx::Error> {{", struct_name), |b| {
                    b.line("let mut results = self.limit(1).fetch_all(pool).await?;");
                    b.line("Ok(results.pop())");
                });
            });
        }

        println!("{}", generated.buf);

        // let out_dir = env::var("OUT_DIR")
        //     .wrap(|| "failed to get out_dir at compile time")?;
        // let dest_path = Path::new(&out_dir).join("orm.rs");
        // fs::write(&dest_path, generated.buf)?;

        Ok(())
    }
}

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

            let orm = Codegen::new_sqlite(pool.clone(), MIGR_DIR)
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

            let orm = Codegen::new_postgres(pool.clone(), MIGR_DIR)
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
