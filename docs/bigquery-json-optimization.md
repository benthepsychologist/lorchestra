# BigQuery JSON Optimization Guide

## Overview

BigQuery has native JSON support, but there are important optimizations for write speed, query performance, and cost at scale.

## Current Implementation

Our event_client currently uses:
- **Streaming Inserts** (`insert_rows_json()`) for event_log
- **MERGE queries** for raw_objects (with JSON payload)

This works well for:
- ✅ Low latency (events written immediately)
- ✅ Simple implementation
- ✅ Moderate volumes (< 1M events/day)

## Optimization Strategies

### 1. Native JSON Type (Already Using)

✅ **We're already doing this!**

```sql
CREATE TABLE raw_objects (
  payload JSON NOT NULL  -- Native JSON type, not STRING
)
```

**Benefits:**
- Efficient storage and compression
- Query JSON fields directly with JSON functions
- Type validation at write time

**Example queries:**
```sql
-- Extract fields from JSON
SELECT
  idem_key,
  JSON_VALUE(payload, '$.id') as message_id,
  JSON_VALUE(payload, '$.subject') as subject
FROM raw_objects
WHERE object_type = 'email'
```

### 2. Clustering (Already Using)

✅ **We're already doing this!**

Both tables are clustered by frequently queried fields:
- `event_log`: Clustered by `source_system, object_type, event_type`
- `raw_objects`: Clustered by `source_system, object_type`

**Benefits:**
- Reduces data scanned during queries
- Improves query performance
- Lowers costs

### 3. Partitioning (Already Using for event_log)

✅ **We're already doing this for event_log!**

```sql
PARTITION BY DATE(created_at)
```

**Benefits:**
- Time-range queries only scan relevant partitions
- Easy data retention (drop old partitions)
- Lower costs for time-based queries

**Consider for raw_objects:**
If you frequently query by time range, add partitioning:
```sql
ALTER TABLE raw_objects
SET OPTIONS (
  partition_expiration_days = 730  -- 2 years
);
```

### 4. Search Indexes (For JSON Fields)

🆕 **BigQuery Search Indexes** can dramatically speed up JSON field searches.

#### When to Use
- Searching within JSON payloads (e.g., email subject/body)
- Text search across many records
- Prefix/substring matching

#### Setup

```sql
-- Create search index on JSON payload
CREATE SEARCH INDEX idx_payload_search
ON `local-orchestration.events_dev.raw_objects`(ALL COLUMNS)
OPTIONS(
  analyzer='LOG_ANALYZER',  -- For log-like data
  -- OR
  -- analyzer='STANDARD',  -- For general text
);
```

**Example usage:**
```sql
-- Search within JSON payloads
SELECT idem_key, payload
FROM `local-orchestration.events_dev.raw_objects`
WHERE SEARCH(payload, 'urgent meeting')
```

### 5. Extracted Columns (Denormalization)

For frequently queried JSON fields, extract them to top-level columns.

#### Example: Email Subject

**Before:**
```sql
SELECT JSON_VALUE(payload, '$.subject') as subject
FROM raw_objects
WHERE object_type = 'email'
```

**After (with extracted column):**
```sql
-- Add column
ALTER TABLE raw_objects
ADD COLUMN email_subject STRING;

-- Update existing rows
UPDATE raw_objects
SET email_subject = JSON_VALUE(payload, '$.subject')
WHERE object_type = 'email';

-- Query
SELECT email_subject
FROM raw_objects
WHERE object_type = 'email'
  AND email_subject LIKE '%urgent%'
```

**Benefits:**
- Faster queries (no JSON parsing)
- Can cluster/partition by extracted fields
- Lower cost (less data scanned)

**Trade-offs:**
- More storage
- Must update extraction logic when ingesting

### 6. BigQuery Storage Write API

🚀 **For high throughput**, migrate from streaming inserts to Storage Write API.

#### Current (Streaming Inserts)
```python
# Simple but slower and more expensive
errors = bq_client.insert_rows_json(table_ref, [envelope])
```

**Characteristics:**
- Latency: < 1 second
- Cost: $0.05 per GB (expensive!)
- Quota: 100,000 rows/sec per table

#### Future (Storage Write API)
```python
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient
from google.cloud.bigquery_storage_v1.types import AppendRowsRequest

# Batch writes - faster and cheaper
write_client = BigQueryWriteClient()
# ... (more complex setup, but 10x faster and cheaper)
```

**Characteristics:**
- Latency: 1-5 seconds (batch)
- Cost: $0.025 per GB (50% cheaper!)
- Throughput: Much higher

**When to migrate:**
- Ingesting > 100K events/day
- Cost optimization needed
- Can tolerate 1-5 second latency

### 7. Batching (Easy Win)

Even with streaming inserts, batch multiple records per API call.

#### Current
```python
# One record per call (inefficient)
for record in records:
    emit(record, ...)  # 1000 records = 1000 API calls
```

#### Optimized
```python
# Batch records (efficient)
batch = []
for record in records:
    batch.append(record)

    if len(batch) >= 500:  # Batch size
        bq_client.insert_rows_json(table_ref, batch)
        batch = []

# Insert remaining
if batch:
    bq_client.insert_rows_json(table_ref, batch)
```

**Benefits:**
- Fewer API calls
- Lower latency (network overhead)
- Same cost

### 8. Materialized Views (For Common Queries)

Create materialized views for frequently accessed JSON fields.

```sql
-- Materialized view with extracted fields
CREATE MATERIALIZED VIEW events_dev.emails_mv
AS
SELECT
  idem_key,
  source_system,
  JSON_VALUE(payload, '$.id') as message_id,
  JSON_VALUE(payload, '$.subject') as subject,
  JSON_VALUE(payload, '$.from') as from_email,
  TIMESTAMP(JSON_VALUE(payload, '$.date')) as email_date,
  first_seen,
  last_seen
FROM raw_objects
WHERE object_type = 'email'
```

**Benefits:**
- Pre-computed results
- Automatic updates
- Much faster queries

**Trade-offs:**
- Extra storage cost
- Refresh lag (typically seconds to minutes)

## Recommended Optimization Path

### Phase 1: Current (MVP) ✅
- [x] Native JSON type
- [x] Clustering
- [x] Partitioning (event_log only)
- [x] MERGE for deduplication

**Good for:** < 1M events/day, moderate query load

### Phase 2: Search & Extract (When Needed)
- [ ] Add search index for full-text search
- [ ] Extract high-cardinality fields (email subject, sender)
- [ ] Add materialized views for dashboards

**Trigger:** Query performance degrades, or need text search

### Phase 3: High Throughput (Scale)
- [ ] Migrate to Storage Write API
- [ ] Implement batching in event_client
- [ ] Add write caching layer (if needed)

**Trigger:** > 1M events/day, cost optimization needed

## Cost Comparison

### Streaming Inserts (Current)
- **Write cost**: $0.05 per GB
- **Query cost**: $6.25 per TB scanned
- **Storage**: $0.02 per GB/month (active), $0.01 per GB/month (long-term)

**Example: 1M emails/day**
- Avg email: 10 KB
- Daily ingestion: 10 GB
- Monthly write cost: 10 GB × 30 days × $0.05/GB = **$15/month**
- Monthly storage: 300 GB × $0.02/GB = **$6/month**
- **Total: ~$21/month**

### Storage Write API (Future)
- **Write cost**: $0.025 per GB (50% cheaper)
- Same query and storage costs

**Example: 1M emails/day**
- Monthly write cost: 10 GB × 30 days × $0.025/GB = **$7.50/month** (50% savings!)

## Monitoring

Track these metrics to know when to optimize:

```sql
-- Query performance
SELECT
  job_id,
  total_bytes_processed / 1e9 as gb_scanned,
  total_slot_ms / 1000 as seconds_runtime
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY total_bytes_processed DESC
LIMIT 10;

-- Table size
SELECT
  table_name,
  size_bytes / 1e9 as size_gb,
  row_count
FROM `local-orchestration.events_dev.__TABLES__`
ORDER BY size_bytes DESC;

-- Streaming insert quota usage
SELECT
  job_type,
  COUNT(*) as job_count,
  SUM(total_bytes_processed) / 1e9 as total_gb
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND job_type = 'LOAD'  -- Streaming inserts
GROUP BY job_type;
```

## Next Steps

1. **Monitor current performance** - Establish baseline metrics
2. **Add search index** - If you need full-text search on emails
3. **Extract common fields** - For high-frequency queries
4. **Batch writes** - Easy 2-3x performance improvement
5. **Consider Storage Write API** - When scale demands it

## References

- [BigQuery JSON Functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions)
- [BigQuery Search Indexes](https://cloud.google.com/bigquery/docs/search-intro)
- [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)
