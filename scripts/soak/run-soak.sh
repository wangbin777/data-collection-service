#!/usr/bin/env bash
set -euo pipefail

POINTS="${POINTS:-10000}"
DEVICES="${DEVICES:-0}"
DURATION_SECONDS="${DURATION_SECONDS:-300}"
COLLECTION_INTERVAL_MS="${COLLECTION_INTERVAL_MS:-1000}"
PROFILE="${PROFILE:-soak}"
SCENARIO="${SCENARIO:-normal}"
MAVEN_REPO_LOCAL="${MAVEN_REPO_LOCAL:-.m2-local}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
TDENGINE_URL="${TDENGINE_URL:-jdbc:TAOS-RS://127.0.0.1:6041/wangbin_collector}"
TDENGINE_USERNAME="${TDENGINE_USERNAME:-root}"
TDENGINE_PASSWORD="${TDENGINE_PASSWORD:-taosdata}"
MQTT_BROKER_URL="${MQTT_BROKER_URL:-tcp://127.0.0.1:1883}"
SPREAD_WITHIN_INTERVAL="${SPREAD_WITHIN_INTERVAL:-true}"
INGRESS_MODE="${INGRESS_MODE:-point}"
HISTORY_CORE_SIZE="${HISTORY_CORE_SIZE:-0}"
HISTORY_MAX_SIZE="${HISTORY_MAX_SIZE:-0}"
HISTORY_QUEUE_CAPACITY="${HISTORY_QUEUE_CAPACITY:-0}"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
if [[ "$DEVICES" == "0" ]]; then
  DEVICES=$(( POINTS / 1000 ))
  if [[ "$DEVICES" -lt 1 ]]; then
    DEVICES=1
  fi
fi
METRICS_OUTPUT="${METRICS_OUTPUT:-target/soak-results/$RUN_ID}"
mkdir -p "$METRICS_OUTPUT" "$MAVEN_REPO_LOCAL"
REDIS_KEY_PREFIX="collector:soak:$RUN_ID"

EXTRA_ARGS=()
if [[ "$HISTORY_CORE_SIZE" != "0" ]]; then
  EXTRA_ARGS+=("-Dcollector.telemetry-executors.history.core-size=$HISTORY_CORE_SIZE")
fi
if [[ "$HISTORY_MAX_SIZE" != "0" ]]; then
  EXTRA_ARGS+=("-Dcollector.telemetry-executors.history.max-size=$HISTORY_MAX_SIZE")
fi
if [[ "$HISTORY_QUEUE_CAPACITY" != "0" ]]; then
  EXTRA_ARGS+=("-Dcollector.telemetry-executors.history.queue-capacity=$HISTORY_QUEUE_CAPACITY")
fi

echo "启动 Soak：points=$POINTS devices=$DEVICES duration=$DURATION_SECONDS scenario=$SCENARIO output=$METRICS_OUTPUT"
mvn -B -ntp \
  "-Dmaven.repo.local=$MAVEN_REPO_LOCAL" \
  -Dtest=RealEnvironmentSoakIT \
  "-Dspring.profiles.active=$PROFILE" \
  -Dcollector.config.loader=file \
  "-Dspring.data.redis.host=$REDIS_HOST" \
  "-Dspring.data.redis.port=$REDIS_PORT" \
  "-Dspring.data.redis.password=$REDIS_PASSWORD" \
  "-Dspring.datasource.url=$TDENGINE_URL" \
  "-Dspring.datasource.username=$TDENGINE_USERNAME" \
  "-Dspring.datasource.password=$TDENGINE_PASSWORD" \
  -Dtelemetry.tdengine.enabled=true \
  "-Dtelemetry.tdengine.buffer.pending-key=$REDIS_KEY_PREFIX:history:pending:v1" \
  "-Dtelemetry.tdengine.buffer.processing-key=$REDIS_KEY_PREFIX:history:processing:v1" \
  "-Dtelemetry.tdengine.buffer.dead-letter-key=$REDIS_KEY_PREFIX:history:dead:v1" \
  "-Dcollector.telemetry-ingress-buffer.pending-key=$REDIS_KEY_PREFIX:telemetry:ingress:pending:v1" \
  "-Dcollector.telemetry-ingress-buffer.processing-key=$REDIS_KEY_PREFIX:telemetry:ingress:processing:v1" \
  "-Dcollector.telemetry-ingress-buffer.dead-letter-key=$REDIS_KEY_PREFIX:telemetry:ingress:dead:v1" \
  "-Dcollector.report.outbox.key-prefix=$REDIS_KEY_PREFIX:cloud:outbox:v1:" \
  "-Dspring.data.redis.stream.key=$REDIS_KEY_PREFIX:telemetry:stream:v1" \
  -Dcollector.report.mqtt.enabled=true \
  "-Dcollector.report.mqtt.broker-url=$MQTT_BROKER_URL" \
  -Dcollector.report.mqtt.gateway-product-key=soak-gateway-pk \
  -Dcollector.report.mqtt.gateway-device-name=soak-gateway \
  "-Dcollector.report.mqtt.client-id=collector-soak-$RUN_ID" \
  -Dcollector.report.cloud.ack.timeout-ms=5000 \
  -Dcollector.report.interval-ms=1000 \
  "-Dsoak.points=$POINTS" \
  "-Dsoak.devices=$DEVICES" \
  "-Dsoak.durationSeconds=$DURATION_SECONDS" \
  "-Dsoak.collectionIntervalMs=$COLLECTION_INTERVAL_MS" \
  "-Dsoak.spreadWithinInterval=$SPREAD_WITHIN_INTERVAL" \
  "-Dsoak.ingressMode=$INGRESS_MODE" \
  "-Dsoak.scenario=$SCENARIO" \
  "-Dsoak.metricsOutput=$METRICS_OUTPUT" \
  "${EXTRA_ARGS[@]}" \
  test
