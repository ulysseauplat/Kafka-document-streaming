# Document Similarity Detection with Kafka + LSH

Detect similar comments posted by the same author to identify spam patterns, such as:
- **Repetitive spam**: Same user posting identical or near-identical comments
- **Multi-article spam**: Same user posting the same comment across different articles

The system uses **Kafka streaming** to process comments in real-time, **LSH** for efficient similarity detection, and **Jaccard similarity** to measure how similar two comments are.

## Architecture

```
                         ┌─────────────┐
    CSV File              │   Kafka     │           ┌───────────┐
     ───────►             │   Cluster   │           │  SQLite   │
                         │ ┌─────────┐ │           │ Database  │
┌──────────┐             │ │ kafka-0 │ │           │           │
│ Producer │────────────►│ │ kafka-1 │ │────┐      │similarities│
└──────────┘             │ └─────────┘ │    │      │   table   │
    │                    └─────────────┘    │      └───────────┘
    │                         │             │              ▲
    │                         ▼             │              │
    │                   ┌────────┬────────┬────────┐       │
    │                   │Consumer│Consumer│Consumer│───────┘
    │                   │   -1   │   -2   │   -3   │
    │                   └────────┴────────┴────────┘
    │
    └──────────────────► S3 Bucket
                       (raw documents)
```

## Key Features

### High Availability
- **2 Kafka brokers** with replication factor 2
- **MIN_ISR = 1**: Topic continues working with only 1 broker
- **3 partitions**: Enables parallel processing

### Parallel Processing
- Multiple consumer instances process partitions concurrently
- Kafka automatically distributes partitions among consumers

```bash
# Run with 3 consumers
docker-compose up -d --scale consumer=3
```

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
- Keeps ~10,000 recent users' shinglings
- Keeps ~50,000 recent similarity pairs
- Old entries are removed to prevent memory exhaustion

**Why this works for spam detection**: Kafka streams are partitioned by `userID` (author key). Since an author typically writes far fewer than 10,000 comments, spammers will be fully contained within this window and won't be missed.

### Monitoring
- **Logs**: Throughput metrics every 10 seconds
- **Kafka-UI**: Dashboard at `http://localhost:8080`

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

The S3 writer is optional - if credentials are not set, documents will only be stored in the local SQLite database.

### Run All Services
```bash
docker-compose up -d
```

### Run with Multiple Consumers
```bash
docker-compose up -d --scale consumer=3
```

### Check Status
```bash
# View all containers
docker-compose ps

# Watch consumer logs
docker logs -f consumer-1

# Check Kafka UI
# Open http://localhost:8080
```

### View Results
```bash
sqlite3 data/similarity.db "SELECT COUNT(*) FROM similarities;"
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

## Project Structure

```
Kafka-document-streaming/
├── docker-compose.yml       # Docker orchestration
├── requirements.txt         # Python dependencies
├── .gitignore
│
├── producer/                # CSV → Kafka
│   ├── main.py             # Reads CSV, streams to Kafka
│   └── Dockerfile
│
├── consumer/                # Kafka → LSH → S3 + Similarity
│   ├── consumer.py         # Main consumer with LSH + S3
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
│   └── s3_writer.py       # S3 document uploader
│
├── database/
│   └── database.py         # SQLite storage for similarities
│
├── data/
│   ├── nyt-comments-part0.csv  # NYT comments dataset
│   └── similarity.db       # Output: similar pairs found
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
