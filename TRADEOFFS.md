# Trade-offs

Due to time constraints and the need for a functional prototype, the following trade-offs were made. These are planned for future implementation in a production environment:

1. **Visibility and Monitoring**:
   - **Current State**: Basic logging to `./logs/pipeline.log` without real-time alerts or detailed metrics (e.g., invalid CDC records, failed transactions).
   - **Future Plan**: Integrate Prometheus for metrics (e.g., transaction counts, errors) and configure email/Slack alerts for pipeline failures.
   - **Reason**: Simplified debugging for development; production requires robust monitoring for mission-critical reliability.

2. **Error Handling for Source Data**:
   - **Current State**: Filters null values in `ingest_cdc.py` but lacks validation for malformed data (e.g., negative amounts, invalid transaction types).
   - **Future Plan**: Add comprehensive validation (e.g., `amount >= 0`, valid `transaction_type`) and quarantine invalid records.
   - **Reason**: Focused on core functionality; production needs robust data quality checks.

3. **Production Database Integration**:
   - **Current State**: Uses PostgreSQL as a temporary queryable copy of Delta tables, not integrated with a production transactional database.
   - **Future Plan**: Recommend Amazon RDS PostgreSQL or Aurora for ACID compliance and scalability. Use Debezium for CDC to sync interest transactions.
   - **Reason**: Simplified local setup; production requires a high-availability database.

4. **Performance Optimization**:
   - **Current State**: No broadcast joins for `rates_df` or caching for large datasets; PostgreSQL writes may be slow for large Delta tables.
   - **Future Plan**: Use `spark.sql.broadcast(rates_df)` and adjust `spark.sql.shuffle.partitions` for scalability.
   - **Reason**: Prioritized functional pipeline; production needs performance tuning.

5. **Security**:
   - **Current State**: Hardcoded credentials (`admin:password`) in `docker-compose.yml` and `sync_to_postgres.py`.
   - **Future Plan**: Use AWS Secrets Manager or environment variables for secure credential management.
   - **Reason**: Simplified development setup; production requires secure practices.

6. **Daily Scheduling**:
   - **Current State**: Pipeline runs manually via `docker-compose up`, missing the requirement for daily runs at 23:59.
   - **Future Plan**: Integrate a scheduler (e.g., Apache Airflow or Dagster) to trigger the pipeline daily.
   - **Reason**: Simplified setup to meet time constraints; production requires automated scheduling.

7. **Improvements on testing**:
   - **Current State**: Tests are unit tests on core methods, missing edge cases (e.g., invalid data, large datasets) and integration tests.
   - **Future Plan**: Improve the test suite with coverage (e.g., pytest-cov), linting (e.g., flake8), and integration tests in a CI/CD pipeline (e.g., GitHub Actions).
   - **Reason**: Focused on basic testing to meet time constraints; production requires a improved setup for reliability and CI/CD integration.