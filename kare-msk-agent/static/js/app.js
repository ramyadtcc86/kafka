let dashboardData = null;
let refreshInterval = null;

document.addEventListener("DOMContentLoaded", () => {
    setupNavigation();
    loadDashboard();
    refreshInterval = setInterval(loadDashboard, 30000);

    document.getElementById("refreshBtn").addEventListener("click", () => {
        refreshAll();
    });
});

function setupNavigation() {
    document.querySelectorAll(".nav-links li").forEach(item => {
        item.addEventListener("click", () => {
            document.querySelectorAll(".nav-links li").forEach(i => i.classList.remove("active"));
            document.querySelectorAll(".tab-content").forEach(t => t.classList.remove("active"));
            item.classList.add("active");
            const tab = item.getAttribute("data-tab");
            document.getElementById("tab-" + tab).classList.add("active");
            loadTabData(tab);
        });
    });
}

function loadTabData(tab) {
    switch(tab) {
        case "overview": loadDashboard(); break;
        case "monitoring": loadMonitoring(); break;
        case "insights": loadInsights(); break;
        case "remediation": loadRemediation(); loadAutoAgentStatus(); break;
        case "cost": loadCost(); break;
        case "alerts": loadAlerts(); break;
        case "testing": loadTesting(); break;
    }
}

async function api(endpoint) {
    try {
        const res = await fetch("/api" + endpoint);
        return await res.json();
    } catch(e) {
        console.error("API error:", endpoint, e);
        return null;
    }
}

async function apiPost(endpoint, body) {
    try {
        const res = await fetch("/api" + endpoint, {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(body)
        });
        return await res.json();
    } catch(e) {
        console.error("API error:", endpoint, e);
        return null;
    }
}

async function loadDashboard() {
    const data = await api("/dashboard");
    if (!data) {
        setStatus("error", "Connection failed");
        return;
    }
    dashboardData = data;

    const cluster = data.cluster;
    if (cluster && !cluster.error) {
        setStatus("connected", "Connected");
        const stateEl = document.getElementById("clusterState");
        stateEl.textContent = cluster.state || "UNKNOWN";
        stateEl.className = "badge badge-" + (cluster.state === "ACTIVE" ? "active" : "warning");

        document.getElementById("brokerCount").textContent = cluster.broker_count || "--";
        document.getElementById("instanceType").textContent = cluster.instance_type || "--";

        renderClusterDetails(cluster);
    } else {
        setStatus("error", cluster?.error || "No data");
        document.getElementById("clusterState").textContent = "UNAVAILABLE";
        document.getElementById("clusterState").className = "badge badge-critical";
        document.getElementById("clusterDetails").innerHTML = `<div class="empty-state"><p>${cluster?.error || "Unable to fetch cluster data. Check AWS credentials."}</p></div>`;
    }

    const score = data.health_score;
    const scoreEl = document.getElementById("healthScore");
    scoreEl.textContent = score + "/100";
    const status = score >= 90 ? "healthy" : score >= 70 ? "warning" : score >= 50 ? "degraded" : "critical";
    document.getElementById("healthStatus").textContent = status.charAt(0).toUpperCase() + status.slice(1);

    const metrics = data.metrics || {};
    if (metrics.cpu) {
        document.getElementById("cpuUsage").textContent = metrics.cpu.value.toFixed(1) + "%";
    }
    if (metrics.disk_used) {
        const diskVal = metrics.disk_used.value;
        const diskUnit = (metrics.disk_used.unit || "").toLowerCase();
        const isPercent = diskUnit.includes("percent") || (diskVal >= 0 && diskVal <= 100 && diskUnit !== "bytes");
        let diskDisplay;
        if (isPercent) {
            diskDisplay = diskVal.toFixed(2) + "%";
        } else if (diskVal >= 1073741824) {
            diskDisplay = (diskVal / 1073741824).toFixed(1) + " GB";
        } else if (diskVal >= 1048576) {
            diskDisplay = (diskVal / 1048576).toFixed(1) + " MB";
        } else if (diskVal >= 1024) {
            diskDisplay = (diskVal / 1024).toFixed(1) + " KB";
        } else {
            diskDisplay = diskVal.toFixed(0) + " B";
        }
        document.getElementById("diskUsage").textContent = diskDisplay;

        const trend = data.disk_trend;
        if (trend) {
            const changeVal = trend.change_value !== undefined ? trend.change_value : trend.change_bytes;
            let changeDisplay;
            if (isPercent) {
                changeDisplay = (changeVal || 0).toFixed(3) + "%";
            } else {
                changeDisplay = formatBytes(changeVal || 0);
            }
            if (trend.direction === "increasing") {
                document.getElementById("diskDetail").innerHTML = `<span style="color:var(--warning)">&#9650; Growing</span> (+${changeDisplay})`;
            } else if (trend.direction === "decreasing") {
                document.getElementById("diskDetail").innerHTML = `<span style="color:var(--success)">&#9660; Shrinking</span> (-${changeDisplay})`;
            } else {
                document.getElementById("diskDetail").innerHTML = `<span style="color:var(--success)">&#9654; Stable</span>`;
            }
        } else {
            document.getElementById("diskDetail").textContent = "storage used";
        }
    }

    renderAlerts(data.alerts || [], "recentAlerts", 5);
    renderInsightsList(data.insights || [], "aiInsights");
}

function renderClusterDetails(c) {
    const el = document.getElementById("clusterDetails");
    const rows = [
        ["Cluster Name", c.cluster_name],
        ["Kafka Version", c.kafka_version],
        ["Instance Type", c.instance_type],
        ["Broker Count", c.broker_count],
        ["Storage/Broker", c.instance_type?.startsWith("express.") ? "Elastic (auto-scaling)" : (c.storage_per_broker_gb + " GB")],
        ["Monitoring", c.enhanced_monitoring],
        ["Encryption", c.encryption_in_transit],
        ["Created", c.creation_time ? new Date(c.creation_time).toLocaleDateString() : "N/A"],
    ];
    el.innerHTML = rows.map(([label, value]) =>
        `<div class="detail-row"><span class="detail-label">${label}</span><span class="detail-value">${value || "N/A"}</span></div>`
    ).join("");
}

function renderAlerts(alerts, containerId, limit) {
    const el = document.getElementById(containerId);
    if (!alerts || alerts.length === 0) {
        el.innerHTML = '<div class="empty-state"><p>No active alerts</p></div>';
        return;
    }
    const items = limit ? alerts.slice(0, limit) : alerts;
    el.innerHTML = items.map(a => `
        <div class="alert-item ${a.severity}">
            <div>
                <div class="alert-title">${a.title}</div>
                <div>${a.message}</div>
                <div class="alert-time">${formatTime(a.timestamp)} | ${a.source || "system"}</div>
            </div>
        </div>
    `).join("");
}

function renderInsightsList(insights, containerId) {
    const el = document.getElementById(containerId);
    if (!insights || insights.length === 0) {
        el.innerHTML = '<div class="empty-state"><p>No insights generated yet. Data is being collected...</p></div>';
        return;
    }
    el.innerHTML = insights.map(i => `
        <div class="insight-card">
            <div class="insight-header">
                <span class="insight-title">${i.title}</span>
                <span class="insight-cat">${i.category}</span>
            </div>
            <div class="insight-desc">${i.description}</div>
            ${i.recommendations && i.recommendations.length > 0 ? `
                <ul class="rec-list">
                    ${i.recommendations.map(r => `<li>${r}</li>`).join("")}
                </ul>
            ` : ""}
        </div>
    `).join("");
}

async function loadMonitoring() {
    const [metricsData, nodesData, alarmsData] = await Promise.all([
        api("/metrics"),
        api("/cluster/nodes"),
        api("/alarms")
    ]);

    const metrics = metricsData?.metrics || {};

    if (metrics.throughput_in) document.getElementById("throughputIn").textContent = formatBytes(metrics.throughput_in.value) + "/s";
    if (metrics.throughput_out) document.getElementById("throughputOut").textContent = formatBytes(metrics.throughput_out.value) + "/s";
    if (metrics.messages_in) {
        const msgVal = metrics.messages_in.value;
        document.getElementById("messagesIn").textContent = msgVal >= 10 ? Math.round(msgVal).toLocaleString() : msgVal.toFixed(1);
    }
    renderMetricsTable(metrics);
    renderPartitionStatus(metrics);

    if (Array.isArray(nodesData)) {
        renderNodes(nodesData);
    } else {
        document.getElementById("clusterNodes").innerHTML = `<div class="empty-state"><p>${nodesData?.error || "No node data available"}</p></div>`;
    }

    if (Array.isArray(alarmsData)) {
        renderCWAlarms(alarmsData);
    } else {
        document.getElementById("cwAlarms").innerHTML = `<div class="empty-state"><p>${alarmsData?.error || "No alarms"}</p></div>`;
    }
}

function renderMetricsTable(metrics) {
    const rows = [
        { label: "CPU User", key: "cpu", unit: "%", max: 100 },
        { label: "CPU System", key: "cpu_system", unit: "%", max: 100 },
        { label: "Memory Used", key: "memory_used", unit: "bytes", max: null },
        { label: "Storage Used", key: "disk_used", unit: "%", max: 100, isPercentAware: true },
        { label: "Produce Latency", key: "produce_latency", unit: "ms", max: null },
        { label: "Fetch Latency", key: "fetch_latency", unit: "ms", max: null },
    ];

    let html = '<table class="metric-table"><thead><tr><th>Metric</th><th>Value</th><th>Status</th></tr></thead><tbody>';
    for (const row of rows) {
        const m = metrics[row.key];
        if (!m) continue;
        let val = m.value;
        if (row.transform) val = row.transform(val);
        const display = row.unit === "bytes" ? formatBytes(val) : (typeof val === "number" ? val.toFixed(1) + (row.unit === "%" ? "%" : " " + row.unit) : val);
        let barHtml = "";
        if (row.max) {
            const pct = Math.min((val / row.max) * 100, 100);
            const cls = pct > 80 ? "fill-danger" : pct > 60 ? "fill-warn" : "fill-good";
            barHtml = `<div class="metric-bar"><div class="metric-bar-fill ${cls}" style="width:${pct}%"></div></div>`;
        }
        html += `<tr><td>${row.label}</td><td>${display}</td><td>${barHtml}</td></tr>`;
    }
    html += "</tbody></table>";
    document.getElementById("brokerHealthTable").innerHTML = html;
}

function renderPartitionStatus(metrics) {
    const el = document.getElementById("partitionStatus");
    const items = [
        { label: "Total Partitions", key: "partition_count" },
        { label: "Total Topics", key: "topic_count" },
        { label: "Under-Replicated", key: "under_replicated" },
        { label: "Offline Partitions", key: "offline_partitions" },
        { label: "Active Controllers", key: "active_controller" },
    ];

    let html = "";
    for (const item of items) {
        const m = metrics[item.key];
        const val = m ? Math.round(m.value).toLocaleString() : "N/A";
        const isDanger = (item.key === "under_replicated" || item.key === "offline_partitions") && m && m.value > 0;
        html += `<div class="detail-row"><span class="detail-label">${item.label}</span><span class="detail-value" style="${isDanger ? 'color: var(--danger)' : ''}">${val}</span></div>`;
    }
    el.innerHTML = html || '<div class="empty-state"><p>No partition data</p></div>';
}

function renderNodes(nodes) {
    const el = document.getElementById("clusterNodes");
    if (!nodes || nodes.length === 0) {
        el.innerHTML = '<div class="empty-state"><p>No node information available</p></div>';
        return;
    }
    el.innerHTML = nodes.map(n => `
        <div class="node-card">
            <div>
                <div class="node-id">Broker ${n.broker_id || "N/A"}</div>
                <div class="node-detail">${n.node_type} | ${n.instance_type || "N/A"}</div>
            </div>
            <div>
                <div class="node-detail">${n.endpoints ? n.endpoints.join(", ") : "No endpoints"}</div>
            </div>
        </div>
    `).join("");
}

function renderCWAlarms(alarms) {
    const el = document.getElementById("cwAlarms");
    if (!alarms || alarms.length === 0) {
        el.innerHTML = '<div class="empty-state"><p>No CloudWatch alarms in ALARM state</p></div>';
        return;
    }
    el.innerHTML = alarms.map(a => `
        <div class="alert-item critical">
            <div>
                <div class="alert-title">${a.name}</div>
                <div>${a.description || a.metric} (threshold: ${a.threshold})</div>
                <div class="alert-time">${formatTime(a.updated)}</div>
            </div>
        </div>
    `).join("");
}

async function loadInsights() {
    const [rca, anomalies, recs, aiCached] = await Promise.all([
        api("/rca"),
        api("/anomalies"),
        api("/recommendations"),
        api("/ai-analysis")
    ]);

    if (rca) {
        let html = `<div class="rca-section"><h4>Summary</h4><p style="font-size:14px;margin-bottom:12px">${rca.summary}</p></div>`;
        if (rca.probable_causes && rca.probable_causes.length > 0) {
            html += `<div class="rca-section"><h4>Probable Causes</h4><ul>${rca.probable_causes.map(c => `<li>${c}</li>`).join("")}</ul></div>`;
        }
        if (rca.recommended_actions && rca.recommended_actions.length > 0) {
            html += `<div class="rca-section"><h4>Recommended Actions</h4><ul>${rca.recommended_actions.map(a => `<li>${a}</li>`).join("")}</ul></div>`;
        }
        document.getElementById("rcaContent").innerHTML = html;
    }

    if (Array.isArray(anomalies)) {
        const producerSummary = anomalies.find(a => a.type === "producer_summary");
        let anomalyHtml = "";

        if (producerSummary && producerSummary.top_producers && producerSummary.top_producers.length > 0) {
            anomalyHtml += `<div style="margin-bottom:8px"><h4 style="font-size:14px;color:var(--text-secondary);margin-bottom:8px">Producer Activity by Topic</h4>`;
            anomalyHtml += `<div style="overflow-x:auto"><table class="metrics-table"><thead><tr>
                <th>Topic</th><th>Bytes In</th><th>Messages In</th><th>% of Total</th><th>Status</th>
            </tr></thead><tbody>`;
            anomalyHtml += producerSummary.top_producers.map(p => `<tr>
                <td style="font-weight:500">${p.topic}</td>
                <td>${p.bytes_in}</td>
                <td>${p.messages_in}</td>
                <td>${p.pct_share}</td>
                <td>${p.status === 'critical' ? '<span class="badge badge-critical">CRITICAL</span>' : p.status === 'warning' ? '<span class="badge badge-warning">WARNING</span>' : '<span class="badge badge-active">Normal</span>'}</td>
            </tr>`).join("");
            anomalyHtml += `</tbody></table></div></div>`;
        }

        const producerAnomalies = anomalies.filter(a => a.type === "producer_anomaly");
        if (producerAnomalies.length > 0) {
            anomalyHtml += `<div style="margin-top:12px"><h4 style="font-size:14px;color:var(--text-secondary);margin-bottom:8px">Producer Anomalies Detected</h4>`;
            anomalyHtml += producerAnomalies.map(a => {
                const cls = a.severity === "critical" ? "critical" : "warning";
                return `<div class="alert-item ${cls}"><div style="width:100%">
                    <div class="alert-title">${a.metric}</div>
                    <div style="font-size:13px">${a.current_value}</div>
                    <div style="font-size:12px;color:var(--accent);margin-top:4px">${a.recommended_action}</div>
                </div></div>`;
            }).join("");
            anomalyHtml += `</div>`;
        }

        if (!anomalyHtml) {
            anomalyHtml = '<div class="empty-state"><p>No producer activity data yet. Waiting for metrics collection.</p></div>';
        }

        document.getElementById("anomalyContent").innerHTML = anomalyHtml;
    }

    if (aiCached && !aiCached.error && aiCached.overall_assessment) {
        renderAIAnalysis(aiCached);
    }

    if (Array.isArray(recs)) {
        if (recs.length === 0) {
            document.getElementById("optimizationContent").innerHTML = '<div class="empty-state"><p>No optimization recommendations at this time</p></div>';
        } else {
            document.getElementById("optimizationContent").innerHTML = recs.map(r => `
                <div class="insight-card">
                    <div class="insight-header">
                        <span class="insight-title">${r.title}</span>
                        <span class="badge badge-${r.impact === 'high' ? 'critical' : r.impact === 'medium' ? 'warning' : 'info'}">${r.impact} impact</span>
                    </div>
                    <div class="insight-desc">${r.description}</div>
                    <div style="font-size:12px;color:var(--accent);margin-top:4px">${r.action}</div>
                </div>
            `).join("");
        }
    }
}

async function refreshAIAnalysis() {
    const btn = document.getElementById("aiRefreshBtn");
    const container = document.getElementById("aiAnalysisContent");
    btn.disabled = true;
    btn.textContent = "Analyzing...";
    container.innerHTML = '<div class="loading">Running AI analysis with AWS Bedrock... This may take a few seconds.</div>';

    try {
        const resp = await fetch("/api/ai-analysis/refresh", { method: "POST" });
        const data = await resp.json();
        renderAIAnalysis(data);
    } catch (e) {
        container.innerHTML = `<div class="empty-state"><p>Error: ${e.message}</p></div>`;
    } finally {
        btn.disabled = false;
        btn.textContent = "Run AI Analysis";
    }
}

async function clearAnomalies(btn) {
    btn.disabled = true;
    btn.textContent = "Clearing...";
    try {
        const res = await fetch("/api/clear-anomalies", { method: "POST" });
        const result = await res.json();
        document.getElementById("anomalyContent").innerHTML = '<div class="empty-state"><p>' + (result.message || "Anomalies cleared.") + '</p></div>';
        btn.textContent = "Cleared!";
        setTimeout(() => { btn.textContent = "Clear Anomalies"; btn.disabled = false; }, 2000);
    } catch (e) {
        btn.textContent = "Clear Anomalies";
        btn.disabled = false;
    }
}

async function clearAICache() {
    const btn = document.getElementById("aiClearBtn");
    const container = document.getElementById("aiAnalysisContent");
    btn.disabled = true;
    btn.textContent = "Clearing...";
    try {
        await fetch("/api/ai-analysis/clear", { method: "POST" });
        container.innerHTML = '<div class="empty-state"><p>AI analysis cache cleared. Click "Run AI Analysis" to generate fresh insights.</p></div>';
        btn.textContent = "Cleared!";
        setTimeout(() => { btn.textContent = "Clear Cache"; btn.disabled = false; }, 2000);
    } catch (e) {
        btn.textContent = "Clear Cache";
        btn.disabled = false;
    }
}

function renderAIAnalysis(data) {
    const container = document.getElementById("aiAnalysisContent");

    if (data.error) {
        container.innerHTML = `<div class="empty-state"><p>${data.error}</p></div>`;
        return;
    }

    let html = "";

    if (data.overall_assessment) {
        html += `<div style="background:var(--bg-hover);padding:14px 16px;border-radius:8px;margin-bottom:16px;border-left:4px solid var(--accent)">
            <div style="font-size:12px;color:var(--accent);font-weight:600;margin-bottom:4px">AI ASSESSMENT</div>
            <div style="font-size:14px;line-height:1.5">${data.overall_assessment}</div>
        </div>`;
    }

    if (data.predictions && data.predictions.length > 0) {
        html += `<div style="margin-bottom:16px"><h4 style="margin-bottom:8px;font-size:14px;color:var(--text-secondary)">Metric Predictions</h4>`;
        html += '<table class="metric-table"><thead><tr><th>Metric</th><th>Current</th><th>Trend</th><th>+1h</th><th>+6h</th><th>+24h</th><th>Risk</th></tr></thead><tbody>';
        html += data.predictions.map(p => {
            const riskCls = p.risk_level === "critical" ? "critical" : p.risk_level === "high" ? "critical" : p.risk_level === "medium" ? "warning" : "active";
            const trendIcon = p.trend === "increasing" ? "&#9650;" : p.trend === "volatile" ? "&#9670;" : "&#9654;";
            const trendColor = p.trend === "increasing" ? "var(--danger)" : p.trend === "volatile" ? "var(--warning)" : "var(--success)";
            return `<tr>
                <td><strong>${p.metric}</strong></td>
                <td>${p.current_value}</td>
                <td style="color:${trendColor}">${trendIcon} ${p.trend}</td>
                <td>${p.prediction_1h}</td>
                <td>${p.prediction_6h}</td>
                <td>${p.prediction_24h}</td>
                <td><span class="badge badge-${riskCls}">${p.risk_level}</span></td>
            </tr>
            <tr><td colspan="7" style="padding:2px 16px 8px;font-size:12px;color:var(--text-secondary);border-top:none">${p.explanation}</td></tr>`;
        }).join("");
        html += '</tbody></table></div>';
    }

    if (data.anomalies && data.anomalies.length > 0) {
        html += `<div style="margin-bottom:16px"><h4 style="margin-bottom:8px;font-size:14px;color:var(--text-secondary)">AI-Detected Anomalies</h4>`;
        html += data.anomalies.map(a => {
            const cls = a.severity === "critical" ? "critical" : a.severity === "warning" ? "warning" : "info";
            let topicTag = a.topic ? `<span class="badge badge-warning" style="margin-left:8px">Topic: ${a.topic}</span>` : "";
            return `<div class="alert-item ${cls}"><div style="width:100%">
                <div class="alert-title">${a.metric}${topicTag}</div>
                <div>${a.description}</div>
            </div></div>`;
        }).join("");
        html += '</div>';
    }

    if (data.fetch_latency_analysis) {
        const fla = data.fetch_latency_analysis;
        const elevated = fla.is_elevated;
        const borderColor = elevated ? "var(--warning)" : "var(--success)";
        const statusBadge = elevated ? '<span class="badge badge-warning">ELEVATED</span>' : '<span class="badge badge-active">NORMAL</span>';
        html += `<div style="margin-bottom:16px"><h4 style="margin-bottom:8px;font-size:14px;color:var(--text-secondary)">Fetch Latency Root Cause Analysis</h4>`;
        html += `<div style="background:var(--bg-hover);padding:14px 16px;border-radius:8px;border-left:4px solid ${borderColor}">`;
        html += `<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px"><span style="font-weight:600">Fetch Latency: ${fla.current_ms}</span>${statusBadge}</div>`;
        if (fla.primary_cause) {
            html += `<div style="margin-bottom:8px"><span style="font-size:12px;color:var(--text-secondary)">PRIMARY CAUSE:</span><div style="font-weight:500;margin-top:2px">${fla.primary_cause}</div></div>`;
        }
        if (fla.evidence) {
            html += `<div style="margin-bottom:8px"><span style="font-size:12px;color:var(--text-secondary)">EVIDENCE:</span><div style="font-size:13px;margin-top:2px">${fla.evidence}</div></div>`;
        }
        if (fla.root_causes && fla.root_causes.length > 0) {
            html += `<div style="margin-bottom:8px"><span style="font-size:12px;color:var(--text-secondary)">POSSIBLE CAUSES (ranked):</span><ol style="margin:4px 0 0 16px;font-size:13px">`;
            fla.root_causes.forEach(c => { html += `<li style="margin-bottom:2px">${c}</li>`; });
            html += `</ol></div>`;
        }
        if (fla.recommended_fix) {
            html += `<div><span style="font-size:12px;color:var(--accent)">RECOMMENDED FIX:</span><div style="font-size:13px;margin-top:2px">${fla.recommended_fix}</div></div>`;
        }
        html += `</div></div>`;
    }

    if (data.recommendations && data.recommendations.length > 0) {
        html += `<div style="margin-bottom:16px"><h4 style="margin-bottom:8px;font-size:14px;color:var(--text-secondary)">AI Recommendations</h4>`;
        html += data.recommendations.map(r => {
            const cls = r.priority === "critical" ? "critical" : r.priority === "high" ? "warning" : "active";
            return `<div class="insight-card">
                <div class="insight-header">
                    <span class="insight-title">${r.title}</span>
                    <span class="badge badge-${cls}">${r.priority}</span>
                </div>
                <div class="insight-desc">${r.description}</div>
                <div style="font-size:12px;color:var(--accent);margin-top:4px">${r.action}</div>
            </div>`;
        }).join("");
        html += '</div>';
    }

    if (data.analyzed_at) {
        html += `<div style="font-size:11px;color:var(--text-secondary);text-align:right;margin-top:8px">Analyzed: ${formatTime(data.analyzed_at)} | Model: ${data.model || "Claude"}</div>`;
    }

    container.innerHTML = html;
}

async function loadRemediation() {
    const [actions, log, ops, aiRecs] = await Promise.all([
        api("/remediation/actions"),
        api("/remediation/log"),
        api("/cluster/operations"),
        api("/remediation/ai-recommendations")
    ]);

    renderAiRecommendations(aiRecs);

    if (Array.isArray(actions)) {
        window._remediationActions = actions;
        document.getElementById("remediationActions").innerHTML = actions.map((a, idx) => `
            <div class="action-card">
                <div class="action-info">
                    <h4>${a.name}</h4>
                    <p>${a.description}</p>
                    <div class="action-current">Current: ${a.current_value} | Risk: ${a.risk}</div>
                </div>
                <button class="btn btn-sm" onclick="openActionModalByIndex(${idx})">Execute</button>
            </div>
        `).join("");
    }

    if (Array.isArray(log)) {
        if (log.length === 0) {
            document.getElementById("remediationLog").innerHTML = '<div class="empty-state"><p>No remediation actions have been executed</p></div>';
        } else {
            let html = '<table class="metric-table"><thead><tr><th>Action</th><th>Status</th><th>Details</th><th>Time</th></tr></thead><tbody>';
            html += log.map(l => `<tr><td>${l.action}</td><td><span class="badge badge-${l.status === 'initiated' ? 'info' : l.status === 'failed' ? 'critical' : 'active'}">${l.status}</span></td><td>${l.details}</td><td>${formatTime(l.timestamp)}</td></tr>`).join("");
            html += "</tbody></table>";
            document.getElementById("remediationLog").innerHTML = html;
        }
    }

    if (Array.isArray(ops)) {
        if (ops.length === 0) {
            document.getElementById("clusterOperations").innerHTML = '<div class="empty-state"><p>No recent cluster operations</p></div>';
        } else {
            let html = '<table class="metric-table"><thead><tr><th>Type</th><th>State</th><th>Started</th><th>Ended</th></tr></thead><tbody>';
            html += ops.map(o => `<tr><td>${o.operation_type}</td><td><span class="badge badge-${o.operation_state === 'UPDATE_COMPLETE' ? 'active' : 'warning'}">${o.operation_state}</span></td><td>${formatTime(o.creation_time)}</td><td>${o.end_time ? formatTime(o.end_time) : "In progress"}</td></tr>`).join("");
            html += "</tbody></table>";
            document.getElementById("clusterOperations").innerHTML = html;
        }
    }
}

function renderAiRecommendations(data) {
    const container = document.getElementById("aiRecommendations");
    const summary = document.getElementById("aiRecommendationsSummary");

    if (!data || !data.recommendations) {
        container.innerHTML = '<div class="empty-state"><p>Waiting for enough metric data to generate AI recommendations...</p></div>';
        return;
    }

    const existingAssessment = document.getElementById("aiAssessmentBox");
    if (existingAssessment) existingAssessment.remove();

    if (data.ai_assessment) {
        const assessmentEl = document.createElement("div");
        assessmentEl.id = "aiAssessmentBox";
        assessmentEl.style.cssText = "background:var(--bg-hover);padding:12px 16px;border-radius:8px;margin-bottom:12px;border-left:4px solid var(--accent)";
        let assessHtml = `<div style="font-size:12px;color:var(--accent);font-weight:600;margin-bottom:4px">BEDROCK AI ASSESSMENT</div>
            <div style="font-size:13px;line-height:1.5">${data.ai_assessment}</div>`;
        if (data.ai_analyzed_at) {
            assessHtml += `<div style="font-size:11px;color:var(--text-secondary);margin-top:6px">Last analyzed: ${formatTime(data.ai_analyzed_at)}</div>`;
        }
        assessmentEl.innerHTML = assessHtml;
        container.parentNode.insertBefore(assessmentEl, container);
    }

    if (data.analysis_summary) {
        const s = data.analysis_summary;
        let badges = [];
        if (s.storage_trend === "growing") badges.push(`<span class="badge badge-warning">Storage: ${s.storage_growth_rate}</span>`);
        if (s.throughput_status === "spiking") badges.push('<span class="badge badge-critical">Throughput Spike</span>');
        if (s.messages_status === "spiking") badges.push('<span class="badge badge-critical">Message Spike</span>');
        if (s.cpu_pressure === "high") badges.push('<span class="badge badge-critical">High CPU</span>');
        else if (s.cpu_pressure === "moderate") badges.push('<span class="badge badge-warning">Moderate CPU</span>');
        if (s.partition_trend && s.partition_trend !== "stable") badges.push(`<span class="badge badge-warning">Partitions: ${s.partition_trend}</span>`);

        const recs = data.recommendations || [];
        const highCount = recs.filter(r => r.severity === "high" || r.severity === "critical").length;
        const medCount = recs.filter(r => r.severity === "medium").length;
        const lowCount = recs.filter(r => r.severity === "low").length;
        if (highCount > 0) badges.push(`<span class="badge badge-critical">${highCount} High Priority</span>`);
        if (medCount > 0) badges.push(`<span class="badge badge-warning">${medCount} Medium Priority</span>`);
        if (lowCount > 0 && highCount === 0 && medCount === 0) badges.push(`<span class="badge badge-info">${lowCount} Low Priority</span>`);

        if (badges.length === 0) badges.push('<span class="badge badge-active">All Normal</span>');
        summary.innerHTML = badges.join(" ");
    }

    if (data.recommendations.length === 0) {
        container.innerHTML = '<div class="empty-state"><p>No issues detected. Cluster metrics are within normal ranges.</p></div>';
        return;
    }

    const severityColors = { critical: "var(--error)", high: "var(--warning)", medium: "#f59e0b", low: "var(--text-secondary)" };
    const typeIcons = { scale_up: "⬆", storage: "💾", investigate: "🔍", ai_insight: "🤖" };

    const typeIcons2 = Object.assign({}, typeIcons, { partition: "📊" });

    let html = data.recommendations.map((rec, idx) => {
        const color = severityColors[rec.severity] || "var(--text-secondary)";
        const icon = typeIcons2[rec.type] || "⚡";
        const hasAction = rec.action_type === "scale_brokers" || rec.action_type === "expand_storage";

        let affectedTopicsHtml = "";
        if (rec.affected_topics && rec.affected_topics.length > 0) {
            affectedTopicsHtml = `<div style="margin-top:6px;padding:8px 12px;background:var(--bg-hover);border-radius:6px">
                <div style="font-size:12px;color:#a78bfa;font-weight:600">Affected Topics</div>
                <div style="font-size:13px;margin-top:4px;display:flex;flex-wrap:wrap;gap:4px">${rec.affected_topics.map(t => `<span class="badge" style="background:rgba(167,139,250,0.15);color:#a78bfa;font-size:12px">${t}</span>`).join("")}</div>
            </div>`;
        }

        return `<div class="insight-card" style="border-left:3px solid ${color}">
            <div class="insight-header">
                <span class="insight-title">${icon} ${rec.title}</span>
                <span class="badge" style="background:${color};color:#fff">${rec.severity.toUpperCase()}</span>
            </div>
            <div class="insight-desc">${rec.description}</div>
            ${affectedTopicsHtml}
            <div style="margin-top:8px;padding:8px 12px;background:var(--bg-hover);border-radius:6px">
                <div style="font-size:12px;color:var(--accent);font-weight:600">Recommended Action</div>
                <div style="font-size:13px;margin-top:4px">${rec.recommended_action}</div>
            </div>
            <div style="margin-top:6px;font-size:12px;color:var(--text-secondary)">
                <span>Impact: ${rec.impact}</span>
                <span style="margin-left:12px">Source: ${rec.source}</span>
            </div>
            ${hasAction ? `<div style="margin-top:8px"><button class="btn btn-sm" onclick="executeAiRecommendation(${idx})" id="aiRecBtn${idx}">Execute Action</button></div>` : ""}
        </div>`;
    }).join("");

    container.innerHTML = html;
    window._aiRecommendations = data.recommendations;
}

async function executeAiRecommendation(idx) {
    const rec = window._aiRecommendations[idx];
    if (!rec || !rec.action_params) return;

    const btn = document.getElementById(`aiRecBtn${idx}`);
    if (!confirm(`Execute: ${rec.recommended_action}?`)) return;

    btn.disabled = true;
    btn.textContent = "Executing...";

    const result = await apiPost("/remediation/execute", {
        action: rec.action_type,
        params: rec.action_params
    });

    if (result?.error) {
        alert("Error: " + result.error);
    } else {
        alert("Action initiated successfully!");
        loadRemediation();
    }

    btn.disabled = false;
    btn.textContent = "Execute Action";
}

async function loadAutoAgentStatus() {
    try {
        const data = await api("/auto-agent/status");
        if (!data) return;

        const badge = document.getElementById("agentStatusBadge");
        const btn = document.getElementById("agentToggleBtn");
        if (data.enabled) {
            badge.className = "badge badge-active";
            badge.textContent = "Enabled";
            btn.textContent = "Disable";
        } else {
            badge.className = "badge badge-critical";
            badge.textContent = "Disabled";
            btn.textContent = "Enable";
        }

        document.getElementById("agentEvalCount").textContent = data.evaluation_count || 0;
        document.getElementById("agentLastEval").textContent = data.last_evaluation ? formatTime(data.last_evaluation) : "--";

        const allActions = [...(data.actions_taken || []), ...(data.actions_skipped || [])];
        allActions.sort((a, b) => (b.timestamp || "").localeCompare(a.timestamp || ""));

        const container = document.getElementById("agentActionLog");
        if (allActions.length === 0) {
            container.innerHTML = '<div class="empty-state"><p>No auto-remediation actions yet. Agent evaluates every 60 seconds.</p></div>';
            return;
        }

        container.innerHTML = allActions.slice(0, 10).map(a => {
            const isApproved = a.executed;
            const borderColor = isApproved ? "var(--accent)" : "var(--warning)";
            const statusBadge = isApproved
                ? '<span class="badge badge-active">AI APPROVED</span>'
                : '<span class="badge badge-warning">SKIPPED (Safety)</span>';
            const icon = isApproved ? "✅" : "⚠️";

            return `<div class="insight-card" style="border-left:3px solid ${borderColor};margin-bottom:8px">
                <div style="display:flex;justify-content:space-between;align-items:center">
                    <span style="font-weight:600">${icon} ${a.action_type.replace(/_/g, " ").toUpperCase()}</span>
                    ${statusBadge}
                </div>
                <div style="font-size:13px;margin-top:6px;color:var(--text-secondary)">${a.details}</div>
                <div style="font-size:11px;margin-top:4px;color:var(--text-secondary)">${formatTime(a.timestamp)}</div>
            </div>`;
        }).join("");
    } catch (e) {}
}

async function toggleAutoAgent() {
    const btn = document.getElementById("agentToggleBtn");
    const badge = document.getElementById("agentStatusBadge");
    const isEnabled = badge.textContent === "Enabled";

    btn.disabled = true;
    btn.textContent = "Updating...";

    const result = await apiPost("/auto-agent/toggle", { enabled: !isEnabled });

    if (result) {
        if (result.enabled) {
            badge.className = "badge badge-active";
            badge.textContent = "Enabled";
            btn.textContent = "Disable";
        } else {
            badge.className = "badge badge-critical";
            badge.textContent = "Disabled";
            btn.textContent = "Enable";
        }
    }
    btn.disabled = false;
}

async function triggerAgentEvaluation() {
    const btn = document.getElementById("agentEvalBtn");
    btn.disabled = true;
    btn.textContent = "Evaluating...";

    const result = await apiPost("/auto-agent/evaluate", {});

    if (result && result.actions && result.actions.length > 0) {
        let msg = `Evaluated ${result.total_topics} topics.\n`;
        if (result.storage_action) {
            msg += `Storage: ${result.storage_action.utilization_pct}% utilized (${result.storage_action.current_gb} GB / ${result.storage_action.limit_gb} GB)\n`;
        }
        msg += `${result.topics_above_threshold} issue(s) detected.\n\n`;

        result.actions.forEach(a => {
            const status = a.action_taken ? "AI APPROVED" : "SKIPPED";
            if (a.action_type === "storage_expansion") {
                msg += `[${status}] Storage Expansion: ${a.current_gb} GB -> ${a.proposed_target_gb} GB\n`;
            } else {
                msg += `[${status}] ${a.topic}: ${a.pct_of_total}% of ingress\n`;
            }
            if (a.ai_safety_check) {
                msg += `  AI Safety: ${a.ai_safety_check.safe_to_execute ? "SAFE" : "UNSAFE"} (${(a.ai_safety_check.confidence * 100).toFixed(0)}% confidence)\n`;
                msg += `  Reason: ${a.ai_safety_check.reasoning}\n`;
            }
        });
        alert(msg);
    } else if (result) {
        let msg = `Evaluated ${result.total_topics || 0} topics.`;
        if (result.storage_action) {
            msg += ` Storage: ${result.storage_action.utilization_pct}% utilized.`;
        }
        msg += ` All within safe limits.`;
        alert(msg);
    }

    btn.disabled = false;
    btn.textContent = "Run Now";
    loadAutoAgentStatus();
    loadRemediation();
}

async function loadCost() {
    const [costData, capacityData] = await Promise.all([
        api("/cost"),
        api("/capacity")
    ]);

    if (costData?.estimated_cost) {
        const cost = costData.estimated_cost;
        const isExpress = cost.broker_type === "Express";
        let breakdownHtml = `
            <div class="cost-item">Compute: <span>$${cost.compute_cost_monthly?.toLocaleString() || "N/A"}</span></div>
            <div class="cost-item">Storage: <span>$${cost.storage_cost_monthly?.toLocaleString() || "N/A"}</span></div>`;
        if (isExpress && cost.data_ingestion_cost_monthly !== undefined) {
            breakdownHtml += `<div class="cost-item">Data Ingestion: <span>$${cost.data_ingestion_cost_monthly?.toLocaleString() || "0"}</span></div>`;
        }

        let detailsHtml = `
            <div class="detail-row"><span class="detail-label">Instance Type</span><span class="detail-value">${cost.instance_type}</span></div>
            <div class="detail-row"><span class="detail-label">Broker Type</span><span class="detail-value">${cost.broker_type || "Standard"}</span></div>
            <div class="detail-row"><span class="detail-label">Brokers</span><span class="detail-value">${cost.broker_count}</span></div>`;
        if (isExpress) {
            detailsHtml += `
                <div class="detail-row"><span class="detail-label">Storage Type</span><span class="detail-value">${cost.storage_type}</span></div>
                <div class="detail-row"><span class="detail-label">Current Storage Used</span><span class="detail-value">${cost.current_storage_used}</span></div>
                <div class="detail-row"><span class="detail-label">Est. Monthly Ingestion</span><span class="detail-value">${cost.estimated_monthly_ingestion_gb?.toLocaleString() || "0"} GB</span></div>`;
        } else {
            detailsHtml += `
                <div class="detail-row"><span class="detail-label">Storage/Broker</span><span class="detail-value">${cost.storage_per_broker_gb || 0} GB</span></div>`;
        }
        detailsHtml += `<div class="detail-row"><span class="detail-label">Hourly Rate/Broker</span><span class="detail-value">$${cost.hourly_rate_per_broker}</span></div>`;

        document.getElementById("costEstimate").innerHTML = `
            <div class="cost-amount">$${cost.total_estimated_monthly?.toLocaleString() || "N/A"}</div>
            <div style="font-size:13px;color:var(--text-secondary)">Estimated monthly cost</div>
            <div class="cost-breakdown">${breakdownHtml}</div>
            <div style="margin-top:12px">${detailsHtml}</div>
        `;
    }

    if (capacityData?.storage) {
        const s = capacityData.storage;
        const pct = s.current_usage_pct || 0;
        const barPct = Math.min(pct, 100);
        const cls = pct > 85 ? "fill-danger" : pct > 70 ? "fill-warn" : "fill-good";
        document.getElementById("storageForecast").innerHTML = `
            <div class="forecast-bar">
                <div class="forecast-bar-fill ${cls}" style="width:${barPct}%">${pct.toFixed(1)}%</div>
            </div>
            <div class="detail-row"><span class="detail-label">Used</span><span class="detail-value">${s.used_display || s.used_gb + "GB"}</span></div>
            <div class="detail-row"><span class="detail-label">${s.storage_type === "Elastic" ? "Elastic Capacity" : "Provisioned Capacity"}</span><span class="detail-value">${s.storage_type === "Elastic" ? "Auto-scaling" : s.total_storage_gb + " GB"}</span></div>
            <div class="detail-row"><span class="detail-label">Daily Growth</span><span class="detail-value">${s.daily_growth_rate || "stable"}</span></div>
            ${s.days_to_90_pct ? `<div class="detail-row"><span class="detail-label">Days to 90%</span><span class="detail-value" style="color:${s.days_to_90_pct < 14 ? 'var(--danger)' : 'var(--text-primary)'}">${s.days_to_90_pct} days</span></div>` : ""}
            ${s.projected_90_pct_date ? `<div class="detail-row"><span class="detail-label">Projected 90% Date</span><span class="detail-value">${new Date(s.projected_90_pct_date).toLocaleDateString()}</span></div>` : ""}
            <div style="margin-top:12px;padding:10px;background:var(--bg-hover);border-radius:6px;font-size:13px">${s.recommendation}</div>
        `;
    }

    if (costData?.recommendations) {
        if (costData.recommendations.length === 0) {
            document.getElementById("costRecommendations").innerHTML = '<div class="empty-state"><p>No cost optimizations identified</p></div>';
        } else {
            document.getElementById("costRecommendations").innerHTML = costData.recommendations.map(r => `
                <div class="insight-card">
                    <div class="insight-header">
                        <span class="insight-title">${r.title}</span>
                        <span class="badge badge-${r.risk === 'high' ? 'critical' : r.risk === 'medium' ? 'warning' : 'active'}">${r.risk} risk</span>
                    </div>
                    <div class="insight-desc">${r.description}</div>
                    <div style="font-size:13px;color:var(--success);font-weight:600;margin-top:4px">Estimated savings: ${r.estimated_savings}</div>
                </div>
            `).join("");
        }
    }

    if (capacityData?.throughput) {
        const t = capacityData.throughput;
        document.getElementById("throughputAnalysis").innerHTML = `
            <div class="detail-row"><span class="detail-label">Total Throughput In</span><span class="detail-value">${formatBytes(t.total_throughput_in_bytes_sec)}/s</span></div>
            <div class="detail-row"><span class="detail-label">Total Throughput Out</span><span class="detail-value">${formatBytes(t.total_throughput_out_bytes_sec)}/s</span></div>
            <div class="detail-row"><span class="detail-label">Per-Broker In</span><span class="detail-value">${formatBytes(t.per_broker_in_bytes_sec)}/s</span></div>
            <div class="detail-row"><span class="detail-label">Per-Broker Out</span><span class="detail-value">${formatBytes(t.per_broker_out_bytes_sec)}/s</span></div>
            <div class="detail-row"><span class="detail-label">Broker Count</span><span class="detail-value">${t.broker_count}</span></div>
        `;
    }

    const cachedAi = await api("/ai-cost-analysis");
    if (cachedAi && !cachedAi.error) {
        renderAiCostAnalysis(cachedAi);
    }
}

async function runAiCostAnalysis() {
    const btn = document.getElementById("runAiCostBtn");
    const container = document.getElementById("aiCostAnalysis");
    btn.disabled = true;
    btn.textContent = "Analyzing...";
    container.innerHTML = '<div class="loading">Running AI cost & capacity analysis... This may take a moment.</div>';

    try {
        const data = await apiPost("/ai-cost-analysis/refresh");
        if (data) {
            renderAiCostAnalysis(data);
        } else {
            container.innerHTML = '<div class="empty-state"><p>AI analysis request failed. Please try again.</p></div>';
        }
    } catch (e) {
        container.innerHTML = `<div class="empty-state"><p>AI analysis failed: ${e.message}</p></div>`;
    } finally {
        btn.disabled = false;
        btn.textContent = "Run AI Analysis";
    }
}

function renderAiCostAnalysis(data) {
    const container = document.getElementById("aiCostAnalysis");

    if (data.error) {
        container.innerHTML = `<div class="empty-state"><p>${data.error}</p></div>`;
        return;
    }

    let html = "";

    if (data.cost_assessment) {
        const ca = data.cost_assessment;
        const ratingColor = ca.rating === "excellent" ? "var(--success)" : ca.rating === "good" ? "var(--accent)" : ca.rating === "fair" ? "var(--warning)" : "var(--danger)";
        html += `<div style="background:var(--bg-hover);padding:14px 16px;border-radius:8px;margin-bottom:16px;border-left:4px solid ${ratingColor}">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                <div style="font-size:12px;color:${ratingColor};font-weight:600">COST EFFICIENCY</div>
                <span class="badge badge-${ca.rating === 'excellent' || ca.rating === 'good' ? 'active' : ca.rating === 'fair' ? 'warning' : 'critical'}">${(ca.rating || "").toUpperCase()}</span>
            </div>
            <div style="font-size:14px;line-height:1.5">${ca.summary || ""}</div>
            ${ca.monthly_cost ? `<div style="font-size:13px;color:var(--text-secondary);margin-top:6px">${ca.monthly_cost}</div>` : ""}
            ${ca.cost_per_gb_ingested ? `<div style="font-size:13px;color:var(--text-secondary)">${ca.cost_per_gb_ingested}</div>` : ""}
        </div>`;
    }

    if (data.cost_optimizations && data.cost_optimizations.length > 0) {
        html += `<div style="margin-bottom:16px"><h4 style="margin-bottom:8px;font-size:14px;color:var(--text-secondary)">AI Cost Optimizations</h4>`;
        html += data.cost_optimizations.map(o => {
            const cls = o.priority === "high" ? "warning" : o.priority === "medium" ? "info" : "active";
            return `<div class="insight-card">
                <div class="insight-header">
                    <span class="insight-title">${o.title}</span>
                    <span class="badge badge-${cls}">${o.priority} priority</span>
                </div>
                <div class="insight-desc">${o.description}</div>
                ${o.estimated_savings ? `<div style="font-size:13px;color:var(--success);font-weight:600;margin-top:4px">Estimated savings: ${o.estimated_savings}</div>` : ""}
                ${o.implementation ? `<div style="font-size:12px;color:var(--accent);margin-top:4px">How: ${o.implementation}</div>` : ""}
                ${o.risk ? `<div style="font-size:12px;color:var(--text-secondary);margin-top:2px">Risk: ${o.risk}</div>` : ""}
            </div>`;
        }).join("");
        html += '</div>';
    }

    if (data.scaling_recommendations && data.scaling_recommendations.length > 0) {
        html += `<div style="margin-bottom:16px"><h4 style="margin-bottom:8px;font-size:14px;color:var(--text-secondary)">Scaling Recommendations</h4>`;
        html += data.scaling_recommendations.map(r => `
            <div class="insight-card">
                <div class="insight-header">
                    <span class="insight-title">${r.action}</span>
                    <span class="badge badge-info">${r.timeframe}</span>
                </div>
                <div class="insight-desc">${r.reason}</div>
                ${r.impact ? `<div style="font-size:12px;color:var(--accent);margin-top:4px">Impact: ${r.impact}</div>` : ""}
            </div>
        `).join("");
        html += '</div>';
    }

    if (data.cost_projection) {
        const cp = data.cost_projection;
        html += `<div style="margin-bottom:16px"><h4 style="margin-bottom:8px;font-size:14px;color:var(--text-secondary)">3-Month Cost Projection</h4>
            <div style="background:var(--bg-hover);padding:12px 16px;border-radius:8px">
                <div class="detail-row"><span class="detail-label">Current Monthly</span><span class="detail-value">${cp.current_monthly || "N/A"}</span></div>
                <div class="detail-row"><span class="detail-label">Month 1</span><span class="detail-value">${cp.month_1 || "N/A"}</span></div>
                <div class="detail-row"><span class="detail-label">Month 2</span><span class="detail-value">${cp.month_2 || "N/A"}</span></div>
                <div class="detail-row"><span class="detail-label">Month 3</span><span class="detail-value">${cp.month_3 || "N/A"}</span></div>
                ${cp.trend ? `<div style="font-size:13px;color:var(--text-secondary);margin-top:8px">${cp.trend}</div>` : ""}
            </div>
        </div>`;
    }

    if (data.analyzed_at) {
        html += `<div style="font-size:11px;color:var(--text-secondary);text-align:right;margin-top:8px">Analyzed: ${formatTime(data.analyzed_at)} | Model: ${data.model || "Llama3 70B"}</div>`;
    }

    container.innerHTML = html;
}

async function loadAlerts() {
    const alerts = await api("/alerts");
    if (Array.isArray(alerts)) {
        renderAlerts(alerts, "allAlerts");
    }
}

function openActionModalByIndex(idx) {
    const action = window._remediationActions[idx];
    if (!action) return;
    window._currentAction = action;

    const modal = document.getElementById("modal");
    document.getElementById("modalTitle").textContent = action.name;

    let html = "";
    for (const p of action.params) {
        html += `<label>${p.description}</label>`;
        html += `<input type="${p.type === 'integer' ? 'number' : 'text'}" id="param-${p.name}" placeholder="${p.description}">`;
    }
    html += `<button class="btn" onclick="executeCurrentAction()">Execute Action</button>`;
    document.getElementById("modalBody").innerHTML = html;
    modal.classList.remove("hidden");
}

function closeModal() {
    document.getElementById("modal").classList.add("hidden");
}

async function executeCurrentAction() {
    const action = window._currentAction;
    if (!action) return;

    const params = {};
    for (const p of action.params) {
        const el = document.getElementById("param-" + p.name);
        let val = el.value;
        if (p.type === "integer") val = parseInt(val);
        params[p.name] = val;
    }

    const result = await apiPost("/remediation/execute", { action: action.id, params });
    closeModal();

    if (result?.error) {
        alert("Error: " + result.error);
    } else {
        alert("Action initiated successfully!");
        loadRemediation();
    }
}

async function refreshAll() {
    document.getElementById("refreshBtn").textContent = "Refreshing...";
    await apiPost("/refresh");
    await loadDashboard();
    document.getElementById("refreshBtn").textContent = "Refresh Data";
}

async function refreshMetrics() {
    await apiPost("/metrics/refresh");
    await loadMonitoring();
}

function setStatus(status, text) {
    const dot = document.getElementById("statusDot");
    dot.className = "dot " + status;
    document.getElementById("statusText").textContent = text;
}

function formatTime(ts) {
    if (!ts) return "N/A";
    try {
        const d = new Date(ts);
        return d.toLocaleString();
    } catch(e) {
        return ts;
    }
}

function formatBytes(bytes) {
    if (!bytes || bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

async function checkTestLambdas() {
    const el = document.getElementById("testLambdaStatus");
    el.innerHTML = '<div class="loading">Checking Lambda deployment status...</div>';
    const data = await api("/test/status");
    if (data?.error) {
        el.innerHTML = `<div class="insight-card" style="border-left:3px solid var(--error)"><div class="insight-desc">Error checking Lambda status: ${data.error}</div></div>`;
        return;
    }
    let html = '<div class="stats-grid four-cols">';
    const nameMap = {
        "msk-test-consume-all": "Consume All Topics",
        "msk-test-consume-lag": "Topic Cleanup",
        "msk-test-storage-growth": "Storage Growth",
        "msk-test-dominant-producer": "Dominant Producer"
    };
    for (const [fname, info] of Object.entries(data)) {
        const name = nameMap[fname] || fname;
        const deployed = info.deployed;
        const color = deployed ? "var(--success)" : "var(--error)";
        const status = deployed ? info.state || "Active" : "Not Deployed";
        html += `<div class="stat-card">
            <div class="stat-label">${name}</div>
            <div class="stat-value" style="font-size:14px;color:${color}">${status}</div>
            ${deployed && info.last_modified ? `<div class="stat-sub">${formatTime(info.last_modified)}</div>` : ""}
        </div>`;
    }
    html += '</div>';
    el.innerHTML = html;
}

async function runTest(testType, params, btn) {
    const origText = btn.textContent;
    if (!confirm(`Run ${testType.replace(/_/g, " ")} test? This will send data to your MSK cluster.`)) return;
    btn.disabled = true;
    btn.textContent = "Starting...";
    const result = await apiPost("/test/invoke", { test_type: testType, params });
    if (result?.error) {
        alert("Error: " + result.error);
        btn.textContent = origText;
        btn.disabled = false;
        return;
    }
    btn.textContent = "Running...";
    btn.style.background = "var(--success)";
    setTimeout(() => {
        btn.textContent = origText;
        btn.disabled = false;
        btn.style.background = "";
    }, 5000);
    alert(result.message || "Test started successfully! Monitor the dashboard tabs for anomalies.");
}

async function runConsumeAll(btn) {
    const perTopic = parseInt(document.getElementById("consumeAllPerTopic").value) || 60;
    const maxTime = parseInt(document.getElementById("consumeAllMaxTime").value) || 240;
    if (!confirm(`Consume messages from ALL user topics in the cluster?\n\nTime per topic: ${perTopic}s\nTotal max time: ${maxTime}s`)) return;
    const origText = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Running...";
    try {
        const resp = await fetch("/api/test/invoke", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ test_type: "consume_all", params: { max_seconds_per_topic: perTopic, max_seconds: maxTime } })
        });
        const result = await resp.json();
        btn.textContent = "Started!";
        btn.style.background = "var(--success)";
        alert(result.message || "Consumer Lambda started. It will consume messages from all topics in the background.");
        setTimeout(() => { btn.textContent = origText; btn.disabled = false; btn.style.background = ""; }, 5000);
    } catch (err) {
        btn.textContent = "Failed";
        btn.style.background = "var(--critical)";
        setTimeout(() => { btn.textContent = origText; btn.disabled = false; btn.style.background = ""; }, 3000);
    }
}

async function runDrainTopic(btn) {
    const topicName = document.getElementById("drainTopicName").value.trim();
    if (!topicName) {
        alert("Please enter a topic name.");
        return;
    }
    const mode = document.getElementById("consumeMode").value;
    const modeLabel = mode === "drain" ? "drain" : mode === "delete_topic" ? "delete" : "drain & delete";
    if (!confirm(`Are you sure you want to ${modeLabel} topic "${topicName}"?`)) return;
    const origText = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Running...";
    try {
        const resp = await fetch("/api/test/invoke", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ test_type: "consume_lag", params: { mode, topic_name: topicName, max_seconds: 240 } })
        });
        const result = await resp.json();
        btn.textContent = "Done!";
        btn.style.background = "var(--success)";
        alert(`Topic "${topicName}" — ${result.status || "completed"}\n${result.consumed !== undefined ? "Messages consumed: " + result.consumed : ""}`);
        setTimeout(() => { btn.textContent = origText; btn.disabled = false; btn.style.background = ""; }, 3000);
    } catch (err) {
        btn.textContent = "Failed";
        btn.style.background = "var(--critical)";
        setTimeout(() => { btn.textContent = origText; btn.disabled = false; btn.style.background = ""; }, 3000);
    }
}

async function clearLagData(btn) {
    if (!confirm("Clear all consumer lag data, related alerts, and remediation log entries from the dashboard?")) return;
    const origText = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Clearing...";
    try {
        const resp = await fetch("/api/clear-lag-data", { method: "POST" });
        const result = await resp.json();
        btn.textContent = "Cleared!";
        btn.style.background = "var(--success)";
        setTimeout(() => {
            btn.textContent = origText;
            btn.disabled = false;
            btn.style.background = "var(--warning)";
        }, 3000);
        alert(result.message || "Lag data cleared from dashboard.");
        refreshData();
    } catch (e) {
        alert("Error: " + e.message);
        btn.textContent = origText;
        btn.disabled = false;
    }
}

async function loadTesting() {
    checkTestLambdas();
}
