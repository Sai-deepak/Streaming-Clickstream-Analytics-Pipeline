#!/bin/bash

echo "=== Kafka Diagnostics ==="

# Check if Kafka services are running
echo "1. Checking Kafka services..."
brew services list | grep -E "(zookeeper|kafka)"

echo ""
echo "2. Checking if Kafka is listening on port 9092..."
lsof -i :9092 || echo "Kafka not listening on port 9092"

echo ""
echo "3. Checking if Zookeeper is listening on port 2181..."
lsof -i :2181 || echo "Zookeeper not listening on port 2181"

echo ""
echo "4. Listing Kafka topics..."
kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo "Cannot connect to Kafka"

echo ""
echo "5. Checking if clickstream topic exists..."
kafka-topics --describe --topic clickstream --bootstrap-server localhost:9092 2>/dev/null || echo "clickstream topic does not exist"

echo ""
echo "6. Creating clickstream topic if it doesn't exist..."
kafka-topics --create --topic clickstream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || echo "Topic already exists or cannot create"

echo ""
echo "7. Final topic list..."
kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo "Still cannot connect to Kafka"

echo ""
echo "=== Diagnostics Complete ==="
echo "If you see errors above, try:"
echo "  brew services restart zookeeper"
echo "  brew services restart kafka"
echo "  Wait 30 seconds then re-run this script"