#!/usr/bin/env bash
# Delete all messages in the given Kafka topics (topics and their config are kept).
#
# Usage:
#   ./clear-message.sh                    # clears transaction-logs and consumer-ddp
#   ./clear-message.sh my-topic other     # clears only the given topics
#
# Env:
#   KAFKA_CONTAINER  docker container running Kafka (default: kafka)
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"

if [ "$#" -gt 0 ]; then
  TOPICS=("$@")
else
  TOPICS=("transaction-logs" "consumer-ddp")
fi

kafka() {
  docker exec -i "$CONTAINER" "$KAFKA_BIN/$1" --bootstrap-server "$BOOTSTRAP" "${@:2}"
}

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "Kafka container '$CONTAINER' is not running (try: docker compose up -d)" >&2
  exit 1
fi

existing_topics="$(kafka kafka-topics.sh --list)"

for topic in "${TOPICS[@]}"; do
  if ! grep -qx "$topic" <<<"$existing_topics"; then
    echo "skip $topic: topic does not exist"
    continue
  fi

  # Build a delete request covering every partition; offset -1 = up to the latest offset
  partitions="$(kafka kafka-get-offsets.sh --topic "$topic" | awk -F: -v t="$topic" '
    { printf "%s{\"topic\":\"%s\",\"partition\":%s,\"offset\":-1}", (NR > 1 ? "," : ""), t, $2 }')"

  echo "{\"partitions\":[$partitions],\"version\":1}" \
    | docker exec -i "$CONTAINER" sh -c "cat > /tmp/delete-records.json \
        && $KAFKA_BIN/kafka-delete-records.sh --bootstrap-server $BOOTSTRAP --offset-json-file /tmp/delete-records.json > /dev/null \
        && rm -f /tmp/delete-records.json"

  echo "cleared $topic"
  kafka kafka-get-offsets.sh --topic "$topic" --time earliest | sed 's/^/  start offset /'
done
