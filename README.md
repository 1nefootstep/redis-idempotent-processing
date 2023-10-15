# Redis idempotent processing

## Description

This project is a simple example of how to use Redis/Redisson to process events.

Specifically, it shows how to update and sum a group of numbers while maintaining idempotency.

Different strategies are used and compared against each other.

## Usage

1. Build the image `docker compose build src/main/resources/docker-redis-cluster`
2. Run `docker compose -f src/main/resources/docker-redis-cluster/docker-compose.yaml up -d` to start Redis cluster
2. Start the spring boot application
3. Run `curl -X GET http://localhost:8080/benchmark/all?testFilePath=test-9-500 -H 'Content-Type: application/json'`
   to run the benchmark

## Different strategies

### Brute force

Data in redis: hash

1. Put an entry into redis hash
2. Get all entries from hash and sum in jvm
3. return aggregated sum

### Variable and pipeline

Data in redis: bucket, hash

1. Pipeline read hash value and bucket value
2. Calculate new aggregated value = old aggregated value - old hash value + new hash value
3. Pipeline write new hash value, new aggregated value to bucket
4. Return new aggregated value without waiting for pipeline to complete

### Variable, pipeline and cache

Data in redis: bucket, hash

1. Read hash value from local and only from redis if not found
2. Read bucket value from local and only from redis if not found
3. Calculate new aggregated value = old aggregated value - old hash value + new hash value
4. Update local cache with new aggregated value
5. Pipeline write new hash value, new aggregated value to bucket
6. Return new aggregated value without waiting for pipeline to complete

### Variable, pipeline and lock

This is needed if different instances contend with each other to modify the same field at the same time.
This is not preferred and should be avoided in the design.

1. Identical to pipeline, but with a lock between the operations

## Benchmark results

Descending by P99 QPS.

For 9 owners, up to 500 entries in the hash, 30,000 events and reprocessing additional ~1440 events.

| Strategy                 | Overall QPS | p99 QPS | Total Time Elapsed (s) |
|--------------------------|-------------|---------|------------------------|
| VariableAndPipeline      | 2954.43     | 2169.30 | 10.64                  |
| VariablePipelineAndCache | 2130.96     | 1357.32 | 14.75                  |
| VariablePipelineAndLock  | 901.15      | 733.42  | 34.89                  |
| bruteForce               | 1041.06     | 550.89  | 30.20                  |

For 9 owners, up to 2000 entries in the hash, 30,000 events and reprocessing additional ~1440 events.

| Strategy                 | Overall QPS | p99 QPS | Total Time Elapsed (s) |
|--------------------------|-------------|---------|------------------------|
| VariableAndPipeline      | 3033.21     | 2173.28 | 10.37                  |
| VariablePipelineAndCache | 2176.71     | 1577.85 | 14.44                  |
| VariablePipelineAndLock  | 912.16      | 758.08  | 34.47                  |
| bruteForce               | 454.95      | 243.46  | 69.11                  |




