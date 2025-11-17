use rust_decimal::Decimal;

const POSTGRES_TEMP_FILES_INFLIGHT: &str = "SELECT ts.spcname AS tablespace, COALESCE(COUNT(size), 0) AS files_total, COALESCE(sum(size), 0) AS bytes_total, 
		COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(modification)), 0) AS max_age_seconds 
		FROM pg_tablespace ts LEFT JOIN (SELECT spcname,(pg_ls_tmpdir(oid)).* FROM pg_tablespace WHERE spcname != 'pg_global') ls ON ls.spcname = ts.spcname 
		WHERE ts.spcname != 'pg_global' GROUP BY ts.spcname";

#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGStorageStats {
    tablespace: Option<String>,
    files_total: Option<i64>,
    bytes_total: Option<Decimal>,
    max_age_seconds: Option<Decimal>,
}
