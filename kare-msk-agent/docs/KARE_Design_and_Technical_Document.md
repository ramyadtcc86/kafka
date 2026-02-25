# KARE - Design and Technical Document

**Kafka AI Remediation Engine**
**Proactive Monitoring Dashboard for Amazon MSK**

---

## 1. Executive Summary

KARE is an AI-powered proactive monitoring and remediation system for Amazon Managed Streaming for Apache Kafka (MSK). It provides real-time cluster monitoring, intelligent anomaly detection, automated root cause analysis, AI-driven remediation with safety evaluation, cost optimization, and capacity planning — all delivered through a web-based dashboard.

The system leverages AWS Bedrock (Llama 3 70B) for intelligent analysis and decision-making, enabling proactive identification and resolution of cluster issues before they impact production workloads.

---

## 2. Architecture Overview

### 2.1 High-Level Architecture

```
+------------------+       +-------------------+       +--------------------+
|                  |       |                   |       |                    |
|   Web Dashboard  | <---> |   Flask Backend   | <---> |   AWS Services     |
|   (Browser)      |       |   (Python/Flask)  |       |   - MSK            |
|                  |       |                   |       |   - CloudWatch     |
+------------------+       +-------------------+       |   - Bedrock AI     |
                                  |                    |   - Lambda         |
                           +------+------+             +--------------------+
                           |             |
                    +------+------+ +----+----+
                    | Background  | | In-Mem  |
                    | Scheduler   | | State   |
                    | (60s cycle) | | Store   |
                    +-------------+ +---------+
```

### 2.2 Technology Stack

| Layer            | Technology                                      |
|------------------|-------------------------------------------------|
| Frontend         | HTML5, CSS3, Vanilla JavaScript                  |
| Backend          | Python 3.11, Flask                               |
| AI/ML            | AWS Bedrock (Llama 3 70B Instruct)               |
| AWS SDKs         | boto3 (MSK, CloudWatch, Bedrock, Lambda, STS)    |
| State Management | Thread-safe in-memory store with file persistence |
| Scheduling       | Python threading (daemon threads)                |
| Test Infra       | AWS Lambda functions (Python 3.12 runtime)       |

### 2.3 Design Principles

1. **AI-First Analysis** — All recommendations and anomaly assessments are powered by Bedrock AI, not static thresholds
2. **Safety-First Remediation** — Every auto-remediation action requires AI safety approval with confidence >= 0.7
3. **Cost-Conscious** — AI analysis is triggered manually to avoid unnecessary Bedrock API costs
4. **Real-Time Monitoring** — Background scheduler collects metrics every 60 seconds
5. **Proactive Detection** — Identifies issues before they impact workloads through trend analysis and AI predictions

---

## 3. System Components

### 3.1 Application Entry Point (`app.py`)

The Flask application serves as the central API gateway and web server. It defines all REST endpoints, initializes the background scheduler, and coordinates between services.

- **Port**: 5000
- **Total Endpoints**: 40+ REST API endpoints
- **Startup Sequence**: Initialize AWS clients -> Start background scheduler -> Serve Flask app

### 3.2 Services Layer

#### 3.2.1 AWS Client Factory (`services/aws_client.py`)

Centralized management of boto3 sessions and clients with STS session token support.

- Caches boto3 clients to avoid repeated initialization
- Supports temporary credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
- Provides `credentials_configured()` helper for startup checks
- `refresh_clients()` to force re-initialization when credentials rotate

#### 3.2.2 MSK Service (`services/msk_service.py`)

Direct interactions with the Amazon MSK control plane.

| Function | Description |
|----------|-------------|
| `get_cluster_info()` | Cluster metadata (instance type, broker count, storage, state) |
| `get_bootstrap_brokers()` | Connection strings for Kafka clients |
| `list_nodes()` | Node inventory with instance details |
| `scale_brokers(target_count)` | Horizontal scaling of broker nodes |
| `update_broker_storage(target_gb)` | EBS volume expansion per broker |
| `list_cluster_operations()` | Pending/completed cluster operations |

#### 3.2.3 CloudWatch Service (`services/cloudwatch_service.py`)

Fetches and processes performance metrics from Amazon CloudWatch.

**Cluster-Wide Metrics Collected:**
- CPU Utilization (User + System)
- JVM Heap Memory (Used / Free)
- Disk Usage (KafkaDataLogsDiskUsed + RootDiskUsed)
- Network Throughput (BytesInPerSec / BytesOutPerSec)
- Messages In Per Second
- Consumer Lag and Offset Lag
- Partition Count, Under-Replicated Partitions, Offline Partitions
- Produce/Fetch Latency (ProduceTotalTimeMsMean / FetchConsumerTotalTimeMsMean)

**Per-Topic Metrics Collected:**
- BytesInPerSec per topic (aggregated across all brokers)
- MessagesInPerSec per topic
- PartitionCount per topic

**Topic Activity Filtering:**
- Topics with latest CloudWatch data point <= 10 bytes/s are considered inactive
- Internal Kafka topics (__consumer_offsets, __amazon_msk_canary) are excluded
- Suppressed topics (deleted but CloudWatch data still draining) are skipped
- Auto-unsuppression when sustained new activity is detected (2+ of last 3 data points active)

#### 3.2.4 Bedrock AI Service (`services/bedrock_service.py`)

Integrates with Amazon Bedrock for intelligent analysis using Llama 3 70B Instruct.

**Capabilities:**
- **Cluster Health Assessment** — Overall health rating with severity classification
- **Anomaly Detection** — AI-identified anomalies with affected metrics and topics
- **Predictive Analysis** — Trend detection and future value projections (1h/6h/24h)
- **Recommendations** — Actionable remediation suggestions with priority and affected topics
- **Cost Analysis** — Cost efficiency rating, optimization suggestions, 3-month projections

**Technical Details:**
- Model: `meta.llama3-70b-instruct-v1:0`
- API: Bedrock Converse API
- Cache: 2-minute TTL to avoid redundant API calls
- Auto-cache invalidation when topics disappear from metrics
- Robust JSON parsing handles trailing commas, single quotes, comments, unquoted keys

#### 3.2.5 Insights Service (`services/insights_service.py`)

Deterministic (non-AI) analysis of cluster health and anomaly detection.

**Health Score Calculation (0-100):**
- CPU > 80%: -20 points; CPU > 60%: -10 points
- Storage > 80%: -25 points; Storage > 60%: -15 points
- Under-replicated partitions > 0: -20 points
- Offline partitions > 0: -30 points
- Fetch latency > 500ms: -15 points; > 200ms: -10 points

**Anomaly Detection:**
- Producer activity analysis: identifies dominant producers (>25% of total ingress)
- Critical threshold: >50% of total ingress share
- High throughput warning: >500 KB/s per topic
- Spike detection: current throughput > 1.5x historical average
- Respects topic suppression list for deleted topics

**Root Cause Analysis:**
- Correlates active alerts with probable causes
- Identifies disk I/O pressure, partition imbalance, CPU saturation
- Generates remediation recommendations per root cause

#### 3.2.6 Auto-Remediation Agent (`services/auto_remediation_agent.py`)

Proactive AI-guided remediation system following a Detect -> Validate -> Act workflow.

**Detection Rules:**
| Condition | Trigger |
|-----------|---------|
| Storage utilization > 50% | Storage expansion evaluation |
| AI predicts storage issue | Storage expansion evaluation |
| Topic ingress > 50% of total | Producer throttling evaluation |
| Topic throughput < 100 bytes/s | Skipped (idle topic) |

**AI Safety Validation:**
- Every proposed action is sent to Bedrock for safety evaluation
- Required: `safe_to_execute: true` AND `confidence >= 0.7`
- Prompt includes: cluster state, current metrics, proposed action, risk assessment
- If AI rejects, action is logged as "SKIPPED (Safety)" with explanation

**Safety Mechanisms:**
- Producer throttle cooldown: 300 seconds per topic
- Storage expansion cooldown: 600 seconds
- Manual enable/disable toggle via dashboard
- All actions logged with full audit trail
- Respects topic suppression list

#### 3.2.7 Remediation Service (`services/remediation_service.py`)

Orchestration layer for manual and AI-recommended remediation actions.

**Available Actions:**
- Scale broker count (horizontal scaling)
- Expand broker storage (EBS volume increase)
- Update monitoring configuration
- Rebalance partitions (rolling restart)

**AI Recommendations:**
- Combines Bedrock analysis with current cluster state
- Provides severity badges, affected topics, and execution commands
- Deduplication of recommendations across multiple analysis sources

#### 3.2.8 Cost Service (`services/cost_service.py`)

Financial analysis and optimization for MSK clusters.

**Pricing Support:**
- Standard broker instances (kafka.m5.large, etc.)
- Express broker instances (express.m7g.large/4xlarge/16xlarge)
- Express pricing: $0.408/hr compute + elastic storage + $0.01/GB data ingestion
- Storage pricing varies by provisioned vs. elastic tiers

**Cost Breakdown:**
- Monthly compute costs per broker
- Monthly storage costs (provisioned or elastic)
- Data ingestion charges (Express brokers only)
- Optimization recommendations for over-provisioned clusters

#### 3.2.9 Capacity Service (`services/capacity_service.py`)

Resource forecasting based on historical trends.

- Storage growth rate calculation with "days until 90% full" prediction
- Per-broker throughput evaluation against instance limits
- Trend indicators: Growing / Shrinking / Stable with change amounts

### 3.3 Background Scheduler (`scheduler/jobs.py`)

The scheduler runs as a daemon thread, executing a collection cycle every 60 seconds.

**Collection Cycle Sequence:**
1. Verify AWS credentials are configured
2. Fetch cluster info from MSK API
3. Collect broker metrics from CloudWatch
4. Collect per-topic metrics from CloudWatch
5. Detect removed topics and clear stale AI caches
6. Calculate health score
7. Run anomaly detection
8. Forecast storage capacity
9. Evaluate auto-remediation agent

**Manual Trigger:**
- `POST /api/refresh` triggers an immediate collection cycle
- Useful for seeing results of recent changes without waiting 60 seconds

### 3.4 State Management (`storage/state.py`)

Thread-safe in-memory state store with file persistence for critical data.

**In-Memory State:**
- Cluster info, broker metrics, consumer groups, topics
- Metric history (60-point rolling window per metric)
- Alerts (100 max), AI insights (50 max), remediation log (50 max)
- Health score, cost insights, capacity forecast
- Topic suppression list

**File Persistence:**
- `storage/.suppressed_topics.json` — Persists across app restarts
- Automatically saved when suppression list changes

### 3.5 Frontend (`templates/dashboard.html`, `static/js/app.js`, `static/css/style.css`)

Single-page dashboard application with dark theme.

**Dashboard Tabs:**

| Tab | Content |
|-----|---------|
| Overview | Cluster status, broker metrics, health score, storage trend, quick stats |
| Monitoring | Detailed metrics, CloudWatch alarms, node inventory, cluster operations |
| AI Insights | AI Analysis (Bedrock), Anomaly Detection, Root Cause Analysis, Recommendations |
| Remediation | Available actions, AI-recommended actions, Auto-Agent status, action log |
| Cost & Capacity | Monthly cost breakdown, optimization suggestions, AI cost analysis, capacity forecast |
| Alerts | Alert feed with severity filtering and acknowledgment |
| Test Scenarios | Lambda test triggers, topic cleanup, Lambda deployment status |

**Key UI Features:**
- Auto-refresh every 30 seconds on active tab
- Manual "Refresh Data" button for immediate update
- "Run AI Analysis" button for on-demand Bedrock analysis
- "Clear Cache" and "Clear Anomalies" buttons for stale data management
- Responsive grid layout with card-based components
- Color-coded severity badges (green/yellow/red/purple)

---

## 4. API Reference

### 4.1 Dashboard & General

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Render main dashboard |
| GET | `/api/status` | AWS configuration status |
| GET | `/api/dashboard` | Dashboard summary (cluster, metrics, health, alerts, trends) |
| POST | `/api/refresh` | Force immediate metrics collection cycle |

### 4.2 Cluster Information

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/cluster` | Cluster metadata |
| GET | `/api/cluster/nodes` | Node inventory |
| GET | `/api/cluster/brokers` | Bootstrap broker strings |
| GET | `/api/cluster/operations` | Cluster operations list |

### 4.3 Metrics & Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/metrics` | Current broker metrics |
| POST | `/api/metrics/refresh` | Force metrics refresh from CloudWatch |
| GET | `/api/metrics/history/<metric>` | Historical data for a specific metric |
| GET | `/api/health` | Cluster health analysis |
| GET | `/api/anomalies` | Detected anomalies |
| GET | `/api/topic-metrics` | Per-topic ingress metrics |
| GET | `/api/alarms` | CloudWatch alarms |
| GET | `/api/logs` | CloudWatch log events |

### 4.4 AI Analysis

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/ai-analysis` | Cached AI analysis |
| POST | `/api/ai-analysis/refresh` | Trigger new Bedrock AI analysis |
| POST | `/api/ai-analysis/clear` | Clear AI analysis cache |
| GET | `/api/rca` | Root cause analysis |
| GET | `/api/recommendations` | Optimization recommendations |
| GET | `/api/insights` | AI insights list |
| GET | `/api/alerts` | Active alerts |

### 4.5 Remediation

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/remediation/actions` | Available remediation actions |
| POST | `/api/remediation/execute` | Execute a remediation action |
| GET | `/api/remediation/log` | Remediation action history |
| GET | `/api/remediation/ai-recommendations` | AI-recommended actions |
| GET | `/api/auto-agent/status` | Auto-agent status and history |
| POST | `/api/auto-agent/toggle` | Enable/disable auto-agent |
| POST | `/api/auto-agent/evaluate` | Manual agent evaluation trigger |
| GET | `/api/auto-agent/history` | Agent action history |

### 4.6 Cost & Capacity

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/cost` | Cost estimates and recommendations |
| GET | `/api/capacity` | Capacity forecast |
| GET | `/api/ai-cost-analysis` | Cached AI cost analysis |
| POST | `/api/ai-cost-analysis/refresh` | Trigger new AI cost analysis |

### 4.7 Testing & Maintenance

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/test/invoke` | Invoke test Lambda function |
| GET | `/api/test/status` | Lambda deployment status |
| POST | `/api/clear-lag-data` | Clear consumer lag data |
| POST | `/api/clear-anomalies` | Clear anomalies, agent history, suppression |

---

## 5. Data Flow

### 5.1 Metrics Collection Flow

```
CloudWatch API
    |
    v
get_broker_metrics()          get_per_topic_metrics()
    |                              |
    v                              v
update_state("broker_metrics")  update_state("topic_metrics")
    |                              |
    +--------- Shared State -------+
    |
    v
analyze_cluster_health()  -->  Health Score
detect_anomalies()        -->  Anomaly List
forecast_storage()        -->  Capacity Forecast
evaluate_and_remediate()  -->  Agent Actions
    |
    v
Dashboard API Endpoints  -->  Frontend Display
```

### 5.2 AI Analysis Flow

```
User clicks "Run AI Analysis"
    |
    v
POST /api/ai-analysis/refresh
    |
    v
analyze_with_bedrock()
    |
    v
_build_metrics_prompt()  -->  Constructs prompt with:
    - Current CPU, memory, disk, throughput
    - Per-topic ingress data
    - Metric history and trends
    - Active alerts
    |
    v
Bedrock Converse API (Llama 3 70B)
    |
    v
Parse JSON response  -->  Handle edge cases:
    - Trailing commas
    - Single quotes
    - Comments in JSON
    - Unquoted keys
    |
    v
Cache result (2-min TTL)
    |
    v
Return to frontend  -->  Display:
    - Overall assessment
    - Anomalies with affected topics
    - Recommendations with severity
    - Predictions (1h/6h/24h)
```

### 5.3 Auto-Remediation Flow

```
Scheduler (every 60s)
    |
    v
evaluate_and_remediate()
    |
    +---> Check Storage
    |         |
    |         v
    |     Utilization > 50% OR AI predicts issue?
    |         |
    |         v (yes)
    |     _ask_ai_safety_check()  -->  Bedrock AI
    |         |
    |         v
    |     Confidence >= 0.7 AND safe_to_execute?
    |         |
    |         v (yes)
    |     update_broker_storage() --> Expand +2GB
    |
    +---> Check Each Topic
              |
              v
          Ingress > 50% of total?
              |
              v (yes)
          _ask_ai_safety_check()  -->  Bedrock AI
              |
              v
          Confidence >= 0.7 AND safe_to_execute?
              |
              v (yes)
          Generate throttle command
              |
              v
          Log action + Set cooldown
```

---


## 6. Configuration

### 6.1 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | AWS IAM access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS IAM secret key |
| `AWS_SESSION_TOKEN` | Yes | STS session token (temporary credentials) |
| `AWS_REGION` | No | AWS region (default: us-east-1) |
| `MSK_CLUSTER_ARN` | No | Full ARN of the MSK cluster |



## 7. Security Considerations

1. **Credentials** — Uses AWS STS temporary credentials with session tokens; no long-lived keys
2. **AI Safety Gates** — Every automated action requires AI approval with minimum confidence threshold
3. **Cooldown Periods** — Prevents repeated actions on the same resource within defined windows
4. **Manual Override** — Auto-agent can be disabled via dashboard toggle at any time
5. **Audit Trail** — All remediation actions (executed or skipped) are logged with timestamps
6. **Read-Only by Default** — Destructive operations (scale, expand, throttle) require explicit triggers


---
