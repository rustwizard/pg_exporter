mod integration_tests {
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::{runners::AsyncRunner, *};
    use testcontainers_modules::postgres::Postgres;

    #[tokio::test]
    async fn test_database_interaction() -> Result<(), Box<dyn std::error::Error>> {
        // 1. Start the PostgreSQL container
        let container = Postgres::default()
            .with_tag("latest") // Specify the image tag
            .start()
            .await?;

        // 2. Get the dynamic host port and construct the database URL
        let host_port = container.get_host_port_ipv4(5432).await?;
        let database_url = &format!(
            "postgresql://postgres:postgres@localhost:{}/postgres", // Default user/password for testcontainers postgres module
            host_port
        );

        // 3. Create a sqlx connection pool
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        // 5. Perform your test logic using the `pool`
        let row: (i32,) = sqlx::query_as("SELECT 1 + 1").fetch_one(&pool).await?;

        assert_eq!(row.0, 2);

        Ok(())
        // The container is automatically stopped when the `container` variable goes out of scope (Drop trait)
    }
}
