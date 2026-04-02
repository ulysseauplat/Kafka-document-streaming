#!/bin/bash

echo "===== DEBUG LOG - $(date) ====="
echo ""

echo "===== 1. FLASK CONTAINER LOGS ====="
docker logs flask 2>&1 | tail -50
echo ""

echo "===== 2. CONSUMER CONTAINER LOGS ====="
docker logs consumer 2>&1 | tail -50
echo ""

echo "===== 3. S3 BUCKET CONTENTS ====="
aws s3 ls s3://kafka-stream-project-ulysse/ --recursive 2>&1 | head -50
echo ""

echo "===== 4. SQLITE DATABASE ====="
echo "Tables:"
sqlite3 /data/similarity.db ".tables" 2>&1
echo ""
echo "Count from similarities:"
sqlite3 /data/similarity.db "SELECT COUNT(*) FROM similarities;" 2>&1
echo ""
echo "Count from doc_users:"
sqlite3 /data/similarity.db "SELECT COUNT(*) FROM doc_users;" 2>&1
echo ""
echo "Count from user_stats:"
sqlite3 /data/similarity.db "SELECT COUNT(*) FROM user_stats;" 2>&1
echo ""

echo "===== 5. API ENDPOINTS ====="
echo "Metrics:"
curl -s http://localhost:5000/api/metrics 2>&1
echo ""
echo ""
echo "Similar Comments:"
curl -s http://localhost:5000/api/similar-comments 2>&1
echo ""
echo ""
echo "Spammers:"
curl -s http://localhost:5000/api/spammers 2>&1
echo ""

echo "===== END DEBUG LOG ====="
