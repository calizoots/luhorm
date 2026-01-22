use std::collections::HashMap;

use convert_case::{Case, Casing};
use luhtwin::{LuhTwin, Wrap};
use serde_json::Value;
// use sqlparser::{dialect::{GenericDialect, PostgreSqlDialect}, parser::Parser};
use sqlx::{postgres::PgRow, sqlite::SqliteRow, Column, Pool, Postgres, Row, Sqlite};

use crate::{Backend, ColumnData, DbType, ForeignKey, LuhParam, LuhRow, Params, SqlNormalise, TableData, TypeMapper};

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

impl From<SqliteRow> for LuhRow {
    fn from(row: SqliteRow) -> Self {
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

pub struct SqliteMapper;

impl TypeMapper for SqliteMapper {
    fn split_sql_statements(&self, sql: &str) -> LuhTwin<Vec<String>> {
        Ok(split_sql_statements(sql))
            
        // let dialect = GenericDialect {};
        
        // let statements = Parser::parse_sql(&dialect, sql)
        //     .wrap(|| "failed to parse sql")?;
        
        // let sql_strings = statements.into_iter()
        //     .map(|stmt| stmt.to_string())
        //     .collect();
        
        // Ok(sql_strings)
    }
    
    fn placeholder(&self, _idx: usize) -> String {
        "?".to_string()
    }

    fn uses_numbered_placeholders(&self) -> bool {
        false
    }

    fn sqlx_type(&self) -> String {
        "SqlitePool".to_string()
    }

    fn sqlx_row_type(&self) -> String {
        "sqlite::SqliteRow".to_string()
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
                    is_return: false,

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

    fn split_sql_statements(&self, sql: &str) -> LuhTwin<Vec<String>> {
        Ok(split_sql_statements(sql))

        // let dialect = PostgreSqlDialect {};
        
        // let statements = Parser::parse_sql(&dialect, sql)
        //     .wrap(|| "failed to parse sql")?;
        
        // let sql_strings = statements.into_iter()
        //     .map(|stmt| stmt.to_string())
        //     .collect();
        
        // Ok(sql_strings)
    }

    fn placeholder(&self, idx: usize) -> String {
        format!("${}", idx + 1)
    }

    fn sqlx_type(&self) -> String {
        "PgPool".to_string()
    }

    fn sqlx_row_type(&self) -> String {
        "postgres::PgRow".to_string()
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
                    is_return: false,

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
        let mut referenced_tables: Vec<ForeignKey> = vec![];

        for table in table_names {
            let cols = self.fetch_all_raw(&format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\"")), None)
                .await
                .wrap(|| format!("failed to get columns for table \"{}\"", table))?;

            let fk_rows = self.fetch_all_raw(
                &format!("PRAGMA foreign_key_list(\"{}\")", table.replace('"', "\"\"")),
                None
            ).await?;
            
            let foreign_keys = Self::TM::foreign_keys_from_rows(&table, &fk_rows)?;

            for key in &foreign_keys {
                referenced_tables.push(key.clone());

                tables.iter_mut()
                    .filter(|t| t.name == key.ref_table)
                    .for_each(|t| {
                        let mut new = key.clone();

                        new.is_return = true;

                        t.fks.push(new)
                    });
            }

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
        let mut referenced_tables: Vec<ForeignKey> = vec![];
        
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

            for key in &foreign_keys {
                referenced_tables.push(key.clone());

                tables.iter_mut()
                    .filter(|t| key.ref_table == t.name)
                    .for_each(|t| {
                        t.fks.push(key.clone());
                    });
            }

            println!("{:?}", foreign_keys);

            tables.push(TableData {
                cols: Self::TM::master_table_columns_from_rows(&cols)?,
                fks: foreign_keys,
                name: table
            });
        }
        
        Ok(tables)
    }
}
