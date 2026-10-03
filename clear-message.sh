#!/usr/bin/env bash
# Delete all messages in the given Kafka topics (topics and their config are kept).
# Each topic is cleared on every cluster (container) where it exists.
#
# Usage:
#   ./clear-message.sh                    # clears transaction-logs (kafka) and consumer-ddp (kafka-sync)
#   ./clear-message.sh my-topic other     # clears only the given topics
#
# Env:
#   KAFKA_CONTAINERS  space-separated Kafka containers to search (default: "kafka kafka-sync")
set -euo pipefail

read -r -a CONTAINERS <<<"${KAFKA_CONTAINERS:-kafka kafka-sync}"
INTERNAL_PORT=29092 # PLAINTEXT listener inside the docker network (see docker-compose.yml)
KAFKA_BIN="/opt/kafka/bin"

if [ "$#" -gt 0 ]; then
  TOPICS=("$@")
else
  TOPICS=("transaction-logs" "consumer-ddp")
fi

kafka() {
  local container="$1" tool="$2"
  docker exec -i "$container" "$KAFKA_BIN/$tool" --bootstrap-server "$container:$INTERNAL_PORT" "${@:3}"
}

clear_topic() {
  local container="$1" topic="$2" partitions

  # Build a delete request covering every partition; offset -1 = up to the latest offset
  partitions="$(kafka "$container" kafka-get-offsets.sh --topic "$topic" | awk -F: -v t="$topic" '
    { printf "%s{\"topic\":\"%s\",\"partition\":%s,\"offset\":-1}", (NR > 1 ? "," : ""), t, $2 }')"

  echo "{\"partitions\":[$partitions],\"version\":1}" \
    | docker exec -i "$container" sh -c "cat > /tmp/delete-records.json \
        && $KAFKA_BIN/kafka-delete-records.sh --bootstrap-server $container:$INTERNAL_PORT --offset-json-file /tmp/delete-records.json > /dev/null \
        && rm -f /tmp/delete-records.json"

  echo "cleared $topic on $container"
  kafka "$container" kafka-get-offsets.sh --topic "$topic" --time earliest | sed 's/^/  start offset /'
}

running="$(docker ps --format '{{.Names}}')"
declare -a ACTIVE=()
for container in "${CONTAINERS[@]}"; do
  if grep -qx "$container" <<<"$running"; then
    ACTIVE+=("$container")
  else
    echo "warn: Kafka container '$container' is not running (try: docker compose up -d)" >&2
  fi
done
if [ "${#ACTIVE[@]}" -eq 0 ]; then
  echo "no Kafka container is running" >&2
  exit 1
fi

for topic in "${TOPICS[@]}"; do
  found=false
  for container in "${ACTIVE[@]}"; do
    if kafka "$container" kafka-topics.sh --list | grep -qx "$topic"; then
      clear_topic "$container" "$topic"
      found=true
    fi
  done
  $found || echo "skip $topic: topic does not exist on ${ACTIVE[*]}"
done
