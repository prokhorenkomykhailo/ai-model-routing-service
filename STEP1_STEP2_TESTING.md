# Step 1 + Step 2 (Topic Intelligence) — Local Testing

This runbook shows how to test:
- **Step 1**: corpus-wide topic clustering → publishes to Kafka `ai-topic-drafts`
- **Step 2**: merge/split refinement → consumes `ai-topic-drafts`, publishes to `ai-topic-refined`

## Prereqs
- Docker running
- Kafka + Redis containers running:
  - `lucid-kafka-1` (`localhost:9092`)
  - `lucid-zookeeper-1` (`localhost:2181`)
  - `lucid-redis` (`localhost:6379`)

Start them (from repo root):
```bash
docker compose -f kafka-compose.yml up -d
docker start lucid-redis || true
```

## Step 2 unit test (fastest)
Runs merge + split logic without Kafka:
```bash
cd ai-model-routing-service
./mvnw -Dtest=TopicMergeSplitServiceTest test
```

## End-to-end: Step 1 (Gemini) → Step 2 → Kafka output
### 1) Ensure Gemini key is available (don’t paste into commands)
Put your key into `lucid-slack-ingestion-service/.env.local`:
```env
GEMINI_API_KEY=YOUR_KEY_HERE
```

### 2) Run service with Step 1 CSV runner enabled
This uses the synthetic CSV and triggers Step 1 once on startup.
```bash
cd /home/maksym/sgh_works/lucid

set -a
source lucid-slack-ingestion-service/.env.local
set +a

export GOOGLE_API_KEY="${GEMINI_API_KEY}"
export REDIS_HOST=localhost
export SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export OTEL_SDK_DISABLED=true
export ANONYMIZATION_SERVICE_ENABLED=false

# Step 1 runner
export TOPIC_CLUSTERING_RUNNER_ENABLED=true
export TOPIC_CLUSTERING_RUNNER_OFFLINE_ENABLED=false
export TOPIC_CLUSTERING_RUNNER_CSV_PATH="/home/maksym/sgh_works/lucid/phase_evaluation_engine/data/Synthetic_Slack_Messages.csv"
export TOPIC_CLUSTERING_RUNNER_TENANT_ID="11111111-1111-1111-1111-111111111111"
export TOPIC_CLUSTERING_RUNNER_WORKSPACE_ID="ws-gemini"
export TOPIC_CLUSTERING_RUNNER_WORKSPACE_NAME="Gemini Workspace"
export TOPIC_CLUSTERING_RUNNER_BATCH_NUMBER=50

# Gemini output needs to be large enough for JSON
export AI_PROVIDERS_GEMINI_MAX_OUTPUT_TOKENS_TEXT_QUERY=12288

# Step 2 split controls (optional)
export TOPIC_MERGE_SPLIT_SPLIT_ENABLED=true
export TOPIC_MERGE_SPLIT_MAX_MESSAGES_PER_CLUSTER=40
export TOPIC_MERGE_SPLIT_SPLIT_CHUNK_SIZE=25

cd ai-model-routing-service
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

### 3) Verify Step 1 output topic (`ai-topic-drafts`)
```bash
docker exec lucid-kafka-1 bash -lc \
  'kafka-console-consumer --bootstrap-server localhost:9092 --topic ai-topic-drafts --from-beginning --max-messages 5'
```

### 4) Verify Step 2 output topic (`ai-topic-refined`)
```bash
docker exec lucid-kafka-1 bash -lc \
  'kafka-console-consumer --bootstrap-server localhost:9092 --topic ai-topic-refined --from-beginning --max-messages 5'
```

### 5) (Recommended) Save + filter the exact batch payloads
```bash
cd /home/maksym/sgh_works/lucid

docker exec lucid-kafka-1 bash -lc \
  'kafka-console-consumer --bootstrap-server localhost:9092 --topic ai-topic-drafts --from-beginning --timeout-ms 10000' \
  > logs/ai-topic-drafts.dump || true

docker exec lucid-kafka-1 bash -lc \
  'kafka-console-consumer --bootstrap-server localhost:9092 --topic ai-topic-refined --from-beginning --timeout-ms 10000' \
  > logs/ai-topic-refined.dump || true

jq -c 'select(.batchId=="ws-gemini:batch_50")' logs/ai-topic-drafts.dump > logs/step1-ws-gemini-batch_50.json
jq -c 'select(.batchId=="ws-gemini:batch_50")' logs/ai-topic-refined.dump > logs/step2-ws-gemini-batch_50.json
```

## Step 1 offline mode (no Gemini required)
If you want deterministic, no-network Step 1 output from the same CSV:
```bash
export TOPIC_CLUSTERING_RUNNER_OFFLINE_ENABLED=true
```
Then start the service the same way; Step 1 will emit drafts with `providerId=offline`, and Step 2 will still run on them.

## Step 2 only (Kafka-driven) — send a synthetic draft event
This bypasses Step 1 and tests Step 2 consumer + producer:
```bash
docker exec -i lucid-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic ai-topic-drafts <<'JSON'
{"eventId":"t1","workspaceId":"ws-test","batchId":"ws-test:batch_1","providerId":"geminiProvider","promptVersion":"v1","createdAt":1766660000,"clusterCount":2,"metadata":{},"clusters":[{"clusterId":"c1","draftTitle":"Alpha","channel":"#general","threadId":"t1","participants":["alice","bob"],"messageIds":["m1","m2"]},{"clusterId":"c2","draftTitle":"Alpha dup","channel":"#general","threadId":"t2","participants":["bob","carol"],"messageIds":["m3"]}]}
JSON

docker exec lucid-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic ai-topic-refined --from-beginning --max-messages 3
```

## Where to look if something fails
- Service logs: `logs/ai-model-routing-service.log` or your redirected run log (e.g. `logs/ai-routing-step1-gemini-step2-run50.log`)
- Step 2 DLQ topic: `ai-topic-drafts-dlq`
