# KARE - Proactive Monitoring Dashboard

## Overview
An AI-powered proactive agent for Amazon Managed Streaming for Apache Kafka (MSK). Provides monitoring, diagnostics, optimization recommendations, auto-remediation, cost analysis, and capacity planning through a web dashboard.

## Project Architecture

### Stack
- **Backend**: Python / Flask (port 5000)
- **Frontend**: HTML/CSS/JS (vanilla, dark theme dashboard)
- **AWS SDKs**: boto3 for MSK, CloudWatch, CloudTrail
- **State**: In-memory cache with thread-safe access

### Directory Structure
```
app.py                  # Flask application entry point
services/
  aws_client.py         # Centralized AWS client factory with STS token support
  msk_service.py        # MSK cluster operations (describe, scale, storage, monitoring)
  cloudwatch_service.py # CloudWatch metrics and logs
  insights_service.py   # AI health analysis, anomaly detection, root cause analysis
  bedrock_service.py    # AWS Bedrock AI-powered metric analysis (Llama3 70B)
  remediation_service.py# Auto-remediation actions with safety guards
  cost_service.py       # Cost estimation and optimization recommendations
  capacity_service.py   # Storage/throughput forecasting
scheduler/
  jobs.py               # Background scheduler for periodic data collection
storage/
  state.py              # Thread-safe in-memory state management
templates/
  dashboard.html        # Main dashboard template (7 tabs)
static/
  css/style.css         # Dark theme styles
  js/app.js             # Frontend application logic
scripts/
  lambdas/
    cpu_throughput_spike.py   # Lambda: CPU/throughput spike test
    consumer_lag.py           # Lambda: Consumer lag test
    storage_growth.py         # Lambda: Storage growth test
    dominant_producer.py      # Lambda: Dominant producer throttling test
    consume_lag_test.py       # Lambda: Drain/consume messages from a user-specified topic
    consume_all_topics.py     # Lambda: Consume messages from all user topics in the cluster
  deploy_test_lambdas.py      # Unified deploy script for all test Lambdas
```

### Key Features
1. **Monitoring & Observability** - Broker health, partitions, consumer lag, throughput
2. **AI Insights** - Health scoring, anomaly detection, root cause analysis, predictive analysis, Bedrock AI predictions
3. **Auto-Remediation** - Scale brokers, expand storage, update monitoring (with safety guards)
4. **Cost Optimization** - Monthly cost estimates, downsizing recommendations
5. **Capacity Planning** - Storage forecasting, throughput analysis
6. **Alerting** - Threshold-based alerts for CPU, disk, partitions, lag
7. **Predictive Analysis** - Trend detection, projected values (1h/6h/24h), risk scoring, threshold ETA

### Environment Variables Required
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_SESSION_TOKEN` - AWS session token (for temporary credentials)
- `AWS_REGION` - AWS region (default: us-east-1)
- `MSK_CLUSTER_ARN` - Full ARN of the MSK cluster

### Recent Changes
- 2026-02-24: Removed statistical checks from AI Recommended Actions — now relies exclusively on Bedrock AI for recommendations and anomaly detection
- 2026-02-24: AI Recommended Actions shows Bedrock overall assessment, recommendations, and anomalies with deduplication
- 2026-02-24: Robust AI JSON parsing for Bedrock responses - handles trailing commas, single quotes, comments, unquoted keys
- 2026-02-24: Added consume_lag_test.py Lambda to drain/consume all messages from lag-test topic
- 2026-02-24: API endpoint /api/clear-lag-data to clear consumer lag metrics, alerts, and remediation logs from dashboard
- 2026-02-24: Auto-agent skips topics with < 100 bytes/s to avoid false positive throttle evaluations on idle topics
- 2026-02-24: Auto-remediation agent now monitors storage utilization and AI predictions for storage issues
- 2026-02-24: Agent auto-expands storage by +2 GB when utilization exceeds 50% OR AI predicts storage issue
- 2026-02-24: Storage expansion uses Bedrock AI safety evaluation before executing MSK API call
- 2026-02-24: Fixed CloudWatch metric: uses KafkaDataLogsDiskUsed (correct for MSK) instead of StorageUsed
- 2026-02-24: KafkaDataLogsDiskUsed collected per-broker with proper percentage unit handling
- 2026-02-24: Agent checks AI analysis, anomaly detection, health scores, and AI recommendations for storage warnings
- 2026-02-24: Added AI Auto-Remediation Agent with Bedrock AI safety evaluation for producer throttling
- 2026-02-24: Agent monitors per-topic ingress, auto-throttles producers exceeding 50% threshold only if AI confirms safety (confidence >= 0.7)
- 2026-02-24: Agent API endpoints: /api/auto-agent/status, /api/auto-agent/toggle, /api/auto-agent/evaluate, /api/auto-agent/history
- 2026-02-24: Agent integrated with scheduler for periodic evaluation every 60 seconds
- 2026-02-24: Agent UI on Remediation tab with enable/disable toggle, run-now button, evaluation count, action log
- 2026-02-24: Agent has 300-second cooldown per topic to prevent repeated throttling
- 2026-02-24: Added per-topic producer analysis to anomaly detection - identifies which producer/topic sends the most data
- 2026-02-24: Removed hardcoded producer throttling recommendations — now relies only on Bedrock AI predictive analysis
- 2026-02-24: Per-topic metrics collection from CloudWatch (BytesInPerSec, MessagesInPerSec per topic) with background scheduler
- 2026-02-24: Bedrock AI prompt includes per-topic producer data for intelligent analysis
- 2026-02-24: New /api/topic-metrics endpoint for per-topic ingress data
- 2026-02-24: Anomaly detection UI shows: Producer Activity by Topic table with monitoring data
- 2026-02-24: Internal Kafka topics (__consumer_offsets, __amazon_msk_canary) filtered out from per-topic metrics display
- 2026-02-24: Per-topic CloudWatch metrics now query with all 3 required dimensions (Cluster Name + Broker ID + Topic) and aggregate across brokers
- 2026-02-24: Fetch latency anomaly detection with root cause analysis (disk I/O, partition count, CPU, consumer config)
- 2026-02-24: Fetch latency AI remediation recommendations with actionable consumer tuning and broker scaling advice
- 2026-02-24: Health score now factors in fetch latency (>200ms warning, >500ms critical)
- 2026-02-24: Proactive AI insights always shown on Remediation tab even when no thresholds exceeded
- 2026-02-24: Added AI-driven remediation recommendations based on storage growth, throughput spikes, and producer activity patterns
- 2026-02-24: AI recommendations now include specific topic names, producer data, partition counts, and affected_topics for every recommendation
- 2026-02-24: Per-topic partition count collection from CloudWatch (PartitionCount metric per topic)
- 2026-02-24: Partition count change detection: tracks increases (e.g. 53→68) and identifies which topics contributed most partitions
- 2026-02-24: AI recommendations enriched with topic-level context: dominant producer name, spiking topics, partition details per topic
- 2026-02-24: Frontend shows "Affected Topics" badges on each AI recommendation card with purple topic tags
- 2026-02-24: Partition trend badge in remediation summary (e.g. "Partitions: increasing (53 → 68)")
- 2026-02-24: AI remediation detects: high producer activity (scale cluster), storage growth (expand storage), unusual message patterns
- 2026-02-24: Remediation tab now shows AI-Recommended Actions with severity badges and execute buttons
- 2026-02-24: Removed capacity planning from AI Insights and AI cost analysis (per user request)
- 2026-02-24: Added AI-powered cost optimization using Bedrock Llama3 70B with manual trigger
- 2026-02-24: AI cost analysis provides: cost efficiency rating, optimization suggestions, scaling recommendations, 3-month cost projections
- 2026-02-24: Fixed Cost & Capacity tab for Express brokers - correct pricing ($0.408/hr), elastic storage, data ingestion costs
- 2026-02-24: Added Express broker support (express.m7g.large/4xlarge/16xlarge) to cost calculations
- 2026-02-24: Cost breakdown now shows compute + storage + data ingestion for Express brokers
- 2026-02-24: Added storage trend indicator (Growing/Shrinking/Stable) to Overview with change amount
- 2026-02-24: Added recommended actions to anomaly detection results
- 2026-02-24: Removed statistical predictive analysis; AI Insights now uses only Bedrock AI (Llama3 70B) for predictions
- 2026-02-24: Added AWS Bedrock AI-powered predictive analysis using Llama3 70B model for intelligent metric predictions
- 2026-02-24: Fixed disk_used metric handling - all services now use bytes (not percentage) with human-readable formatting (MB/GB)
- 2026-02-24: Enhanced anomaly detection with statistical analysis (2-sigma spike detection) and proper byte formatting
- 2026-02-24: Complete rebuild from CLI script to full web dashboard with AI agent capabilities
- Centralized AWS client factory handles STS session tokens
- Background scheduler collects metrics every 60 seconds
- Dashboard with 6 tabs: Overview, Monitoring, AI Insights, Remediation, Cost & Capacity, Alerts

### Technical Notes
- disk_used metric from CloudWatch (StorageUsed) is in bytes, not percentage
- Anomaly detection thresholds: Storage > 100GB, CPU > 70%, Consumer Lag > 100,000ms
- Express brokers have elastic storage (auto-scaling), no provisioned capacity; costs based on actual usage
- Express brokers have additional data ingestion charge ($0.01/GB)
- Storage capacity defaults to 100 GB for non-Express if storage_per_broker_gb is not reported by API
- Bedrock AI analysis uses Llama3 70B (meta.llama3-70b-instruct-v1:0) via converse API with 2-minute cache
- AI analysis is triggered manually via "Run AI Analysis" button to avoid unnecessary API costs
