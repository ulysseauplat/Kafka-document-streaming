# Document Similarity Detection with Kafka + LSH

Detect similar comments posted by the same author to identify spam patterns, such as:
- **Repetitive spam**: Same user posting identical or near-identical comments
- **Multi-article spam**: Same user posting the same comment across different articles

The system uses **Kafka streaming** to process comments in real-time, **LSH** for efficient similarity detection, and **Jaccard similarity** to measure how similar two comments are.

## Architecture

```
                          ┌─────────────────────────────────────────────────────┐
     CSV File              │              Kafka Cluster                        │
      ───────►             │  ┌─────────────┐    ┌─────────────┐              │
                          │  │  kafka-0    │    │  kafka-1    │              │
 ┌──────────┐             │  └─────────────┘    └─────────────┘              │
 │ Producer │────────────►│                                                  │
 └──────────┘             └─────────────────────────────────────────────────┘
                                              │             
                                              ▼             
                 ┌──────────────────────────────────────────────┐
                 │              Kafka Topic (3 partitions)        │
                 └──────────────────────────────────────────────┘
                                              │             
              ┌────────────────────────────────┼────────────────┐
              ▼                                ▼                ▼
       ┌────────────┐                  ┌────────────┐   ┌────────────┐
       │ Consumer 1 │                  │ Consumer 2 │   │ Consumer 3 │
       │ • LSH      │                  │ • LSH      │   │ • LSH      │
       │ • MinHash  │                  │ • MinHash  │   │ • MinHash  │
       │ • S3 Writer│                  │ • S3 Writer│   │ • S3 Writer│
       └────────────┘                  └────────────┘   └────────────┘
              │                                │                │
              └────────────────┬──────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
            ┌───────────────┐          ┌───────────────┐
            │  PostgreSQL   │          │      S3       │
            │   Database    │          │   Data Lake   │
            │               │          │               │
            │• similarities │          │• Raw comment  │
            │• doc_users   │          │  documents    │
            │• user_stats  │          └───────┬───────┘
            │• consumer_   │                  │
            │  stats       │                  │
            │• system_     │                  │
            │  stats       │                  │
            └───────┬───────┘                  │
                    │                         │
                    ▼                         ▼
           ┌───────────────────────────────────────┐
           │              Flask UI                  │
           │             (Port 5000)               │
           │                                        │
            │  Metrics from PostgreSQL                  │
           │  Comments display from S3              │
           └───────────────────────────────────────┘
```

### PostgreSQL Database Tables

| Table | Columns | Description |
|-------|---------|-------------|
| `similarities` | `id`, `doc_id_1`, `doc_id_2`, `similarity` | Pairs of similar documents |
| `doc_users` | `doc_id`, `user_id` | Maps document to author |
| `user_stats` | `user_id`, `total_comments`, `similar_pairs`, `similarity_rate` | Per-user aggregated stats |
| `consumer_stats` | `consumer_id`, `timestamp`, `throughput`, `processed_count` | Consumer performance metrics |

## Key Features

### Batch Processing for Performance
To maximize throughput, the consumer uses **batch processing** for both Kafka commits and database writes:

- **Kafka commits**: Offsets are committed every 100 messages or 5 seconds (whichever comes first)
- **Database writes**: Operations are buffered and flushed every 100 messages or 10 seconds (whichever comes first)
- **Batch DB functions**: Instead of individual INSERT/UPDATE per message, we use bulk operations:
  - `batch_track_doc_users()` - Bulk insert document-user mappings
  - `batch_insert_similarities()` - Bulk insert similar pairs
  - `batch_update_user_stats_comments()` - Bulk increment comment counts
  - `batch_update_user_stats_similarities()` - Bulk update similarity stats


### PostgreSQL for Concurrent Access
The system uses **PostgreSQL** instead of SQLite to support multiple concurrent consumers:
- PostgreSQL handles concurrent writes from multiple consumer instances
- Row-level locking ensures data consistency
- Connection pooling-ready design

SQLite would cause locking issues with multiple consumers writing simultaneously.

### High Availability
- **2 Kafka brokers** with replication factor 2
- **MIN_ISR = 1**: Topic continues working with only 1 broker
- **3 partitions**: Enables parallel processing

### Parallel Processing
- Multiple consumer instances process partitions concurrently
- Kafka automatically distributes partitions among consumers


### At-Least-Once Processing
- **Manual offset commit** after each message batch
- If a consumer crashes, messages are reprocessed
- Guarantees **no data loss** (may have duplicates)

### Local Similarity (Spam Detection)
- Messages are keyed by `userID` (routed to same partition)
- Similarity is computed **only within the same author's comments**
- Detects if a user posts the **same message across multiple articles**

### Memory Management
The consumer limits memory usage by periodically cleaning up old data:
- Keeps ~10,000 recent shinglings (configurable via `SHINGLE_DICT_MAX_SIZE`)
- Keeps ~50,000 recent similarity pairs (configurable via `SEEN_PAIRS_MAX_SIZE`)
- LSH index is cleared when switching to a new user (reduces memory overhead)
- Old entries are removed to prevent memory exhaustion

**Why this works for spam detection**: Kafka streams are partitioned by `userID` (author key). Since an author typically writes far fewer than 10,000 comments, spammers will be fully contained within this window and won't be missed.

### Monitoring
- **Logs**: Throughput metrics every 10 seconds
- **Kafka-UI**: Kafka cluster dashboard at `http://localhost:8080`
- **Flask Dashboard**: Real-time metrics and spam detection at `http://localhost:5000`

### Dashboard Metrics Explained

The Flask UI displays real-time metrics sourced from PostgreSQL database:

| Metric | Description |
|--------|-------------|
| **Total Comments** | Total number of comments seen |
| **Processed** | Comments actually processed (excludes comments smaller than shingle size) |
| **Total Similarities** | Total number of similar comment pairs detected |
| **Similarity Rate** | Computed per user: `similar_pairs / (n*(n-1)/2)` where n = user's total comments |
| **Throughput** | Messages processed per second |

**Similarity Rate Formula:**
```
For a user with n comments:
  Possible pairs = n * (n-1) / 2
  Similarity Rate = similar_pairs / possible_pairs
```

### Flask UI

The Flask dashboard reads data from two sources:

- **Metrics & Analytics**: Computed from PostgreSQL database
- **Comment Text**: Retrieved from S3 data lake

Access the Flask UI at `http://localhost:5000`:
- **Dashboard**: Real-time metrics (total comments, similarities found, throughput)
- **Spam Detection**: View authors with high similarity rates and their comments
- **Similar Comments**: View pairs of similar comments with text content

## Algorithm Overview

The system uses **Locality Sensitive Hashing (LSH)** to efficiently find similar documents:

1. **Shingling**: Convert text into k-word tokens (default: k=4)
2. **MinHash**: Convert tokens into a compact signature (50 hash functions)
3. **Banding**: Split signature into bands to find candidate pairs
4. **Jaccard**: Verify actual similarity (threshold: 0.7)

This approach avoids comparing every document pair (O(n²)) by only comparing documents that hash to the same bucket.

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- AWS account (optional, for S3 document storage)

### Test Dataset

For testing purposes, a sample CSV file (`test-spam-dataset.csv`) is included in the `data/` directory.

### AWS Credentials

To enable S3 document storage, create a `.env` file in the project root:
```bash
cp .template .env
```

Then edit `.env` and add your AWS credentials:
```bash
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_REGION=eu-north-1
S3_BUCKET=your-bucket-name
```

The S3 writer is enabled by default - if AWS credentials are not set, S3 writes are skipped but the system continues to function with PostgreSQL only.

### Run All Services
```bash
sudo docker-compose up -d --build
```

### Check Status
```bash
# View all containers
docker-compose ps

# Watch consumer logs
docker logs -f consumer-1

# Check Kafka UI (Kafka cluster)
# Open http://localhost:8080

# Check Flask Dashboard (metrics & spam detection)
# Open http://localhost:5000
```

### View Results
```bash
# Query PostgreSQL database
docker exec -it postgres psql -U postgres -d similarity_db -c "SELECT COUNT(*) FROM similarities;"
```

### Stop Everything
```bash
docker-compose down
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `kafka:9092` | Kafka broker address |
| `SAMPLE_SIZE` | `30000` | Rows to process (0 = all) |
| `AWS_ACCESS_KEY_ID` | - | AWS access key (optional) |
| `AWS_SECRET_ACCESS_KEY` | - | AWS secret key (optional) |
| `AWS_REGION` | `us-east-1` | AWS region |
| `S3_BUCKET` | `kafka-stream-project-ulysse` | S3 bucket name |

### Algorithm Parameters (in `shared/config.py`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SHINGLE_SIZE` | 4 | Words per shingle |
| `NUM_HASHES` | 50 | MinHash signature length |
| `LSH_BANDS` | 10 | Number of LSH bands |
| `SIMILARITY_THRESHOLD` | 0.7 | Min Jaccard similarity to store |

### Resource Limits

| Component | Memory Limit |
|-----------|-------------|
| Kafka brokers | 768MB each |
| Producer | 512MB |
| Consumer | 512MB |
| Flask UI | 256MB |

## Project Structure

```
Kafka-document-streaming/
├── docker-compose.yml       # Docker orchestration
├── requirements.txt         # Python dependencies
├── .gitignore
│
├── producer/                # CSV → Kafka
│   ├── main.py             # Reads CSV, streams to Kafka
│   ├── __init__.py
│   └── Dockerfile
│
├── consumer/                # Kafka → LSH → S3 + Similarity
│   ├── consumer.py         # Main consumer with LSH + S3
│   ├── __init__.py
│   └── Dockerfile
│
├── flask-ui/                # Flask Dashboard
│   ├── app.py              # Flask application with API
│   ├── templates/          # HTML templates
│   │   ├── dashboard.html      # Real-time metrics dashboard
│   │   ├── spammers.html       # Spam detection page
│   │   └── similar-comments.html # Similar comments viewer
│   ├── static/             # Static assets
│   │   └── style.css       # Professional styling
│   └── Dockerfile
│
├── lsh/                     # Locality Sensitive Hashing
│   ├── lsh_index.py        # LSH banding technique
│   ├── minhash.py          # MinHash signature generation
│   └── preprocess.py        # Text preprocessing & k-shingling
│
├── shared/
│   ├── config.py           # Kafka and algorithm configuration
│   ├── s3_config.py        # AWS S3 configuration
│   └── s3_writer.py        # S3 document uploader
│
├── database/
│   └── database.py         # PostgreSQL storage for similarities & user stats
│
├── data/
│   └── test-spam-dataset.csv  # Sample spam dataset for testing
│
└── tests/
    ├── test_lsh.py
    └── test_database.py
```

## Troubleshooting

### Producer can't find Kafka
- Wait for Kafka to be ready (producer waits up to 40 seconds)
- Check logs: `docker logs producer`

### Out of memory
- Reduce `SAMPLE_SIZE` for testing
- Check memory usage: `docker stats`

### No similarities found
- Verify messages are being produced: check Kafka UI
- Lower `SIMILARITY_THRESHOLD` in config (e.g., 0.5)

### Reset state
```bash
# Remove all containers and data
docker-compose down -v

# Rebuild and restart
docker-compose up -d --build
```
