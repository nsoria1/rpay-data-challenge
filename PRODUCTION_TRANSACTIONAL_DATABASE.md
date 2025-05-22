# Recommendations for Ingesting CDI Bonus Data into a Production Database

This document outlines two approaches to ingest CDI Bonus pipeline data from Delta tables into a production transactional database: real-time CDC and batch incremental exports.

## Overview
The CDI Bonus pipeline generates:
- **Wallet History**: Daily user balances.
- **Transactions**: CDC and interest transactions.

## Approach 1: Real-Time CDC with Kafka and Debezium

### How It Works
- **Capture Changes**: Use Debezium to monitor Delta table changes (inserts, updates, deletes).
- **Stream Changes**: Debezium sends changes to Kafka topics as a real-time stream.
- **Sync to Database**: A Kafka Connect sink writes the changes to the production database.
- **Frequency**: Near real-time.

## Approach 2: Batch Incremental Exports

### How It Works
- **Identify Changes**: Use Delta table versioning to find new or updated records since the last export.
- **Extract Changes**: Run a Spark job to read these changes (If using Delta in Production we can do `deltaTable.history()` to track changes).
- **Write to Database**: Write the changes to the production database using JDBC.
- **Frequency**: Scheduled.