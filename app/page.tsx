"use client";

import React, { useEffect, useState, useRef } from "react";

interface EngineLog {
  id: string;
  timestamp: string;
  source: string;
  message: string;
  level: "info" | "warn" | "err";
}

interface EngineMetrics {
  activeRequests: number;
  requestsThroughputSec: number;
  avgInferenceLatencyMs: number;
  queueDepth: number;
  workerUtilization: number;
  recentLogs: EngineLog[];
}

export default function InferXDashboard() {
  const [metrics, setMetrics] = useState<EngineMetrics>({
    activeRequests: 0,
    requestsThroughputSec: 0.0,
    avgInferenceLatencyMs: 0.0,
    queueDepth: 0,
    workerUtilization: 0.0,
    recentLogs: [],
  });

  const [prompt, setPrompt] = useState("");
  const [model, setModel] = useState("local-ml");
  const [apiKey, setApiKey] = useState("sk-valid-key");
  const [output, setOutput] = useState("// Real-time execution results will project here...");
  const [isExecuting, setIsExecuting] = useState(false);
  const [throughputHistory, setThroughputHistory] = useState<number[]>(Array(15).fill(0));
  const [latencyHistory, setLatencyHistory] = useState<number[]>(Array(15).fill(0));
  const [uptimeSeconds, setUptimeSeconds] = useState(0);

  const logListRef = useRef<HTMLDivElement>(null);

  // 1. Initialize Server-Sent Events (SSE) Telemetry Stream
  useEffect(() => {
    const eventSource = new EventSource("/api/telemetry");

    eventSource.onmessage = (event) => {
      try {
        const data: EngineMetrics = JSON.parse(event.data);
        setMetrics(data);

        // Update sparkline histories
        setThroughputHistory((prev) => [...prev.slice(1), data.requestsThroughputSec]);
        setLatencyHistory((prev) => [...prev.slice(1), data.avgInferenceLatencyMs]);
      } catch (err) {
        console.error("Failed to parse telemetry SSE packet:", err);
      }
    };

    eventSource.onerror = (err) => {
      console.warn("Telemetry SSE connection lost. Reconnecting...", err);
    };

    // Increment local uptime clock
    const uptimeTimer = setInterval(() => {
      setUptimeSeconds((prev) => prev + 1);
    }, 1000);

    return () => {
      eventSource.close();
      clearInterval(uptimeTimer);
    };
  }, []);

  // Auto-scroll log console
  useEffect(() => {
    if (logListRef.current) {
      logListRef.current.scrollTop = logListRef.current.scrollHeight;
    }
  }, [metrics.recentLogs]);

  // Execute Playground Inference Request
  const handleExecuteInference = async () => {
    if (!prompt.trim()) {
      alert("Please enter a query prompt.");
      return;
    }

    setIsExecuting(true);
    setOutput("// Processing real-time inference request via /api/inference...");

    try {
      const res = await fetch("/api/inference", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey,
        },
        body: JSON.stringify({ prompt, model }),
      });

      const data = await res.json();
      setOutput(JSON.stringify(data, null, 2));
    } catch (err: any) {
      setOutput(`// Execution Failure:\n${err?.message || "Connection failed"}`);
    } finally {
      setIsExecuting(false);
    }
  };

  const formatUptime = (totalSecs: number) => {
    const hrs = String(Math.floor(totalSecs / 3600)).padStart(2, "0");
    const mins = String(Math.floor((totalSecs % 3600) / 60)).padStart(2, "0");
    const secs = String(totalSecs % 60).padStart(2, "0");
    return `${hrs}:${mins}:${secs}`;
  };

  return (
    <div style={{ backgroundColor: "#060913", color: "#f3f4f6", minHeight: "100vh", fontFamily: "sans-serif" }}>
      {/* Header */}
      <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "1.2rem 2.5rem", borderBottom: "1px solid rgba(255,255,255,0.08)", background: "rgba(6,9,19,0.9)" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
          <div style={{ width: "12px", height: "12px", background: "#00f2fe", borderRadius: "50%", boxShadow: "0 0 12px #00f2fe" }} />
          <h1 style={{ fontSize: "1.4rem", fontWeight: 700, background: "linear-gradient(135deg, #00f2fe, #4facfe)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>
            InferX Operations Center (Real-Time SSE)
          </h1>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "1.5rem" }}>
          <div style={{ background: "rgba(16, 185, 129, 0.15)", border: "1px solid rgba(16, 185, 129, 0.3)", padding: "0.3rem 0.8rem", borderRadius: "20px", color: "#10b981", fontSize: "0.85rem", fontWeight: 600 }}>
            ● LIVE SSE ENGINE
          </div>
          <div style={{ fontSize: "0.85rem", color: "#9ca3af" }}>
            Uptime: <span style={{ color: "#f3f4f6", fontFamily: "monospace" }}>{formatUptime(uptimeSeconds)}</span>
          </div>
        </div>
      </header>

      {/* Main Content Grid */}
      <main style={{ padding: "2rem", display: "grid", gridTemplateColumns: "1fr 380px", gap: "2rem", maxWidth: "1600px", margin: "0 auto" }}>
        {/* Left Column */}
        <div style={{ display: "flex", flexDirection: "column", gap: "2rem" }}>
          {/* Metrics Grid */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "1.5rem" }}>
            {/* Active Throughput (RPS) */}
            <div style={{ background: "rgba(22, 28, 45, 0.45)", border: "1px solid rgba(0,242,254,0.15)", borderRadius: "12px", padding: "1.25rem" }}>
              <div style={{ fontSize: "0.8rem", color: "#9ca3af", textTransform: "uppercase", fontWeight: 600 }}>Active Throughput</div>
              <div style={{ fontSize: "1.8rem", fontWeight: 700, marginTop: "0.4rem" }}>
                {metrics.requestsThroughputSec.toFixed(1)} <span style={{ fontSize: "0.9rem", color: "#9ca3af" }}>RPS</span>
              </div>
              <div style={{ fontSize: "0.75rem", color: "#10b981", marginTop: "0.5rem" }}>Real-Time 5s Window</div>
            </div>

            {/* Average Latency (ms) */}
            <div style={{ background: "rgba(22, 28, 45, 0.45)", border: "1px solid rgba(79,172,254,0.15)", borderRadius: "12px", padding: "1.25rem" }}>
              <div style={{ fontSize: "0.8rem", color: "#9ca3af", textTransform: "uppercase", fontWeight: 600 }}>Avg Latency</div>
              <div style={{ fontSize: "1.8rem", fontWeight: 700, marginTop: "0.4rem" }}>
                {metrics.avgInferenceLatencyMs.toFixed(1)} <span style={{ fontSize: "0.9rem", color: "#9ca3af" }}>ms</span>
              </div>
              <div style={{ fontSize: "0.75rem", color: "#4facfe", marginTop: "0.5rem" }}>Moving Execution Avg</div>
            </div>

            {/* GPU / CPU Load */}
            <div style={{ background: "rgba(22, 28, 45, 0.45)", border: "1px solid rgba(255,255,255,0.05)", borderRadius: "12px", padding: "1.25rem" }}>
              <div style={{ fontSize: "0.8rem", color: "#9ca3af", textTransform: "uppercase", fontWeight: 600 }}>Engine Load</div>
              <div style={{ fontSize: "1.8rem", fontWeight: 700, marginTop: "0.4rem", color: "#00f2fe" }}>
                {Math.round(metrics.workerUtilization * 100)}%
              </div>
              <div style={{ fontSize: "0.75rem", color: "#9ca3af", marginTop: "0.5rem" }}>Host CPU Utilization</div>
            </div>

            {/* Concurrency / Active Connections */}
            <div style={{ background: "rgba(22, 28, 45, 0.45)", border: "1px solid rgba(255,255,255,0.05)", borderRadius: "12px", padding: "1.25rem" }}>
              <div style={{ fontSize: "0.8rem", color: "#9ca3af", textTransform: "uppercase", fontWeight: 600 }}>Concurrency</div>
              <div style={{ fontSize: "1.8rem", fontWeight: 700, marginTop: "0.4rem" }}>
                {metrics.activeRequests}
              </div>
              <div style={{ fontSize: "0.75rem", color: "#9ca3af", marginTop: "0.5rem" }}>Active Connections</div>
            </div>
          </div>

          {/* Playground Panel */}
          <div style={{ background: "rgba(22, 28, 45, 0.45)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: "12px", padding: "1.5rem", display: "flex", flexDirection: "column", gap: "1rem" }}>
            <h2 style={{ fontSize: "1.1rem", fontWeight: 700, borderBottom: "1px solid rgba(255,255,255,0.05)", paddingBottom: "0.5rem" }}>
              Inference Playground (Live Telemetry Trigger)
            </h2>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
              <div>
                <label style={{ fontSize: "0.8rem", color: "#9ca3af", display: "block", marginBottom: "0.3rem" }}>Engine Target</label>
                <select value={model} onChange={(e) => setModel(e.target.value)} style={{ width: "100%", padding: "0.6rem", background: "#060913", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "6px", color: "#fff" }}>
                  <option value="local-ml">InferX-LocalML-v1.0 (ONNX Linear Core)</option>
                  <option value="gemini-2.5-flash">gemini-2.5-flash (Google GenAI)</option>
                </select>
              </div>
              <div>
                <label style={{ fontSize: "0.8rem", color: "#9ca3af", display: "block", marginBottom: "0.3rem" }}>API Bearer Key</label>
                <input type="text" value={apiKey} onChange={(e) => setApiKey(e.target.value)} style={{ width: "100%", padding: "0.6rem", background: "#060913", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "6px", color: "#fff" }} />
              </div>
            </div>

            <div>
              <label style={{ fontSize: "0.8rem", color: "#9ca3af", display: "block", marginBottom: "0.3rem" }}>Input Query Prompt</label>
              <textarea value={prompt} onChange={(e) => setPrompt(e.target.value)} rows={3} placeholder="Enter your prompt query here..." style={{ width: "100%", padding: "0.6rem", background: "#060913", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "6px", color: "#fff" }} />
            </div>

            <button onClick={handleExecuteInference} disabled={isExecuting} style={{ background: "linear-gradient(135deg, #00f2fe, #4facfe)", color: "#060913", border: "none", borderRadius: "6px", padding: "0.8rem", fontWeight: 700, cursor: "pointer" }}>
              {isExecuting ? "Executing Tensor Calculation..." : "Execute Inference Request"}
            </button>

            <div>
              <label style={{ fontSize: "0.8rem", color: "#9ca3af", display: "block", marginBottom: "0.3rem" }}>Live Output Terminal</label>
              <pre style={{ background: "rgba(6,9,19,0.8)", border: "1px solid rgba(255,255,255,0.05)", borderRadius: "6px", padding: "1rem", color: "#00ff66", fontFamily: "monospace", fontSize: "0.85rem", maxHeight: "200px", overflowY: "auto", whiteSpace: "pre-wrap" }}>
                {output}
              </pre>
            </div>
          </div>
        </div>

        {/* Right Column: Live Logs */}
        <div style={{ display: "flex", flexDirection: "column", gap: "1.5rem" }}>
          <div style={{ background: "rgba(22, 28, 45, 0.45)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: "12px", padding: "1.25rem", height: "100%", display: "flex", flexDirection: "column" }}>
            <h3 style={{ fontSize: "1rem", fontWeight: 700, marginBottom: "0.75rem", borderBottom: "1px solid rgba(255,255,255,0.05)", paddingBottom: "0.5rem" }}>
              Live Telemetry Log Feed (SSE)
            </h3>
            <div ref={logListRef} style={{ flex: 1, background: "rgba(6,9,19,0.9)", border: "1px solid rgba(255,255,255,0.05)", borderRadius: "6px", padding: "0.75rem", fontFamily: "monospace", fontSize: "0.78rem", overflowY: "auto", maxHeight: "540px", display: "flex", flexDirection: "column", gap: "0.4rem" }}>
              {metrics.recentLogs.length === 0 ? (
                <div style={{ color: "#6b7280", textAlign: "center", paddingTop: "2rem" }}>Waiting for telemetry logs...</div>
              ) : (
                metrics.recentLogs.map((log) => (
                  <div key={log.id} style={{ lineHeight: 1.35 }}>
                    <span style={{ color: "#6b7280" }}>[{log.timestamp}]</span>{" "}
                    <span style={{ color: "#00f2fe", fontWeight: 600 }}>{log.source}</span>{" "}
                    <span style={{ color: log.level === "err" ? "#ef4444" : log.level === "warn" ? "#f59e0b" : "#f3f4f6" }}>
                      {log.message}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
