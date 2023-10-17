# Redis idempotent processing

## Description

This project is a simple example of how to use Redis/Redisson to process events.

Specifically, it shows how to update and sum a group of numbers while maintaining idempotency.

Different strategies are used and compared against each other.

## Usage

1. Build the image `docker compose build src/main/resources/docker-redis-cluster`
2. Run `docker compose -f src/main/resources/docker-redis-cluster/docker-compose.yaml up -d` to start Redis cluster
2. Start the spring boot application
3.

Run `curl -X GET http://localhost:8080/benchmark/all?testFilePath=scenario1-9-500 -H 'Content-Type: application/json'`
to run the benchmark

## Benchmark results

### Scenario 1: Asset aggregation

Descending by P99 QPS.

For 9 owners, up to 500 entries in the hash, 30,000 events and reprocessing additional ~1440 events.

| Strategy                 | Overall QPS | p99 QPS | Time Elapsed (s) | Speedup |
|--------------------------|-------------|---------|------------------|---------|
| VariableAndPipeline      | 2954.43     | 2169.30 | 10.64            | 2.84x   |
| VariablePipelineAndCache | 2130.96     | 1357.32 | 14.75            | 2.05x   |
| VariablePipelineAndLock  | 901.15      | 733.42  | 34.89            | 0.87x   |
| bruteForce               | 1041.06     | 550.89  | 30.20            | 1.00x   |

For 9 owners, up to 2000 entries in the hash, 30,000 events and reprocessing additional ~1440 events.

| Strategy                 | Overall QPS | p99 QPS | Time Elapsed (s) | Speedup |
|--------------------------|-------------|---------|------------------|---------|
| VariableAndPipeline      | 3033.21     | 2173.28 | 10.37            | 6.66x   |
| VariablePipelineAndCache | 2176.71     | 1577.85 | 14.44            | 4.79x   |
| VariablePipelineAndLock  | 912.16      | 758.08  | 34.47            | 2.00x   |
| bruteForce               | 454.95      | 243.46  | 69.11            | 1.00x   |

### Scenario 2: Number of unique members

For 9 owners, up to 100 businesses, up to 1000 users, 30,000 events and reprocessing additional ~1440 events.

| Strategy                            | Overall QPS | p99 QPS | Time Elapsed (s) | Speedup |
|-------------------------------------|-------------|---------|------------------|---------|
| MemberAggregateByHashCountAndScript | 5480.26     | 3770.21 | 5.74             | 13.78x  |
| MemberAggregateBySetMerge           | 754.12      | 351.52  | 41.69            | 1.90x   |
| MemberAggregateBySetMergeInJvm      | 397.64      | 185.99  | 79.07            | 1.00x   |

For 9 owners, up to 500 businesses, up to 1000 users, 30,000 events and reprocessing additional ~1440 events.

| Strategy                            | Overall QPS | p99 QPS | Time Elapsed (s) | Speedup |
|-------------------------------------|-------------|---------|------------------|---------|
| MemberAggregateByHashCountAndScript | 5497.31     | 3776.32 | 5.72             | 34.27x  |
| MemberAggregateBySetMerge           | 278.95      | 127.18  | 112.71           | 1.74x   |
| MemberAggregateBySetMergeInJvm      | 160.38      | 91.13   | 196.04           | 1.00x   |

## Scenarios

### Scenario 1: Asset aggregation

There is an owner that can own multiple businesses. Each business can have assets of a total certain value.
The task is to calculate the total asset of the owner. The assets of the owner can be updated with an `AssetEvent`.

#### Brute force

Data in redis: hash

1. Put an entry into redis hash
2. Get all entries from hash and sum in jvm
3. return aggregated sum

#### Variable and pipeline

Data in redis: bucket, hash

1. Pipeline read hash value and bucket value
2. Calculate new aggregated value = old aggregated value - old hash value + new hash value
3. Pipeline write new hash value, new aggregated value to bucket
4. Return new aggregated value without waiting for pipeline to complete

#### Variable, pipeline and cache

Data in redis: bucket, hash

1. Read hash value from local and only from redis if not found
2. Read bucket value from local and only from redis if not found
3. Calculate new aggregated value = old aggregated value - old hash value + new hash value
4. Update local cache with new aggregated value
5. Pipeline write new hash value, new aggregated value to bucket
6. Return new aggregated value without waiting for pipeline to complete

#### Variable, pipeline and lock

This is needed if different instances contend with each other to modify the same field at the same time.
This is not preferred and should be avoided in the design.

1. Identical to pipeline, but with a lock between the operations

### Scenario 2: Number of unique members

Similarly to scenario 1, owners own multiple businesses. Each business can have members.
The task is to calculate both the total number of members of each business and each owner.

#### Business set and merge sets in jvm

1. Maintain a set of members for each business, where the size of the set is
   the number of members of the business.
2. Merge the sets of all businesses owned by the same owner in jvm to get the total number of members of the owner.

#### Business set and merge sets in redis

1. Maintain a set of members for each business, where the size of the set is
   the number of members of the business.
2. Merge the sets of all businesses owned by the same owner in redis to get the total number of members of the owner.

#### Business set and owner hash (lua script)

This approach is only possible with lua script or transactions
to maintain atomicity as there are two variables where one depends on another
before updating.

1. Maintain a set of members for each business, where the size of the set is
   the number of members of the business.
2. Meanwhile, maintain a hash where the key is the member
   and the value is the number of businesses the member is a member of.
    - When there is a new member, check if the key exists, and add 1 to the value.
    - When a customer stops being a member, subtract one from the value. If the value is zero,
      remove the key.
    - The size of the hash is the number of unique members of the owner.