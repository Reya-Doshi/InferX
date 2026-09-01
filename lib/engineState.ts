// lib/engineState.ts
/**
 * In-Memory Metrics Store for InferX Telemetry Pipeline.
 * Tracks live request concurrency, rolling throughput window, latency averages, and engine logs.
 */

export interface EngineLog {
  id: string;
  timestamp: string;
  source: string;
  message: string;
  level: "info" | "warn" | "err";
}

export interface EngineMetrics {
  activeRequests: number;
  requestsThroughputSec: number;
  avgInferenceLatencyMs: number;
  queueDepth: number;
  workerUtilization: number;
  recentLogs: EngineLog[];
}

class EngineStateStore {
  private activeRequests: number = 0;
  private requestTimestamps: number[] = [];
  private latencies: number[] = [];
  private recentLogs: EngineLog[] = [];
  private maxLogs: number = 50;

  constructor() {
    // Initial bootstrap log
    this.addLog("gateway", "InferX real-time SSE telemetry pipeline initialized.", "info");
  }

  /**
   * Called when an inference request starts.
   */
  public recordRequestStart(): void {
    this.activeRequests += 1;
    const now = Date.now();
    this.requestTimestamps.push(now);
  }

  /**
   * Called when an inference request finishes (success or error).
   */
  public recordRequestEnd(
    durationMs: number,
    logMsg: string,
    source: string = "gateway",
    isError: boolean = false
  ): void {
    if (this.activeRequests > 0) {
      this.activeRequests -= 1;
    }
    this.latencies.push(durationMs);

    // Keep rolling window of last 100 latencies
    if (this.latencies.length > 100) {
      this.latencies.shift();
    }

    this.addLog(source, logMsg, isError ? "err" : "info");
  }

  /**
   * Adds an entry to the log list.
   */
  public addLog(source: string, message: string, level: "info" | "warn" | "err" = "info"): void {
    const timestamp = new Date().toLocaleTimeString();
    const id = `log-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`;
    this.recentLogs.push({ id, timestamp, source: `[${source}]`, message, level });

    if (this.recentLogs.length > this.maxLogs) {
      this.recentLogs.shift();
    }
  }

  /**
   * Calculates rolling metrics over the last 5 seconds.
   */
  public getMetrics(): EngineMetrics {
    const now = Date.now();
    const windowMs = 5000;

    // Filter timestamps within the last 5 seconds
    this.requestTimestamps = this.requestTimestamps.filter((ts) => now - ts <= windowMs);

    // Real requests per second (RPS) over 5s window
    const rps = Number((this.requestTimestamps.length / (windowMs / 1000)).toFixed(2));

    // Moving average latency
    let avgLatency = 0;
    if (this.latencies.length > 0) {
      const sum = this.latencies.reduce((acc, l) => acc + l, 0);
      avgLatency = Number((sum / this.latencies.length).toFixed(2));
    }

    // Dynamic worker utilization ratio based on active concurrency & RPS
    const utilization = this.activeRequests > 0 || rps > 0
      ? Math.min(Number((0.15 + rps * 0.08 + this.activeRequests * 0.12).toFixed(2)), 0.98)
      : 0.0;

    return {
      activeRequests: this.activeRequests,
      requestsThroughputSec: rps,
      avgInferenceLatencyMs: avgLatency,
      queueDepth: this.activeRequests > 1 ? this.activeRequests - 1 : 0,
      workerUtilization: utilization,
      recentLogs: [...this.recentLogs],
    };
  }
}

// Global Singleton to maintain state across HMR reloads in Next.js development
const globalForEngine = globalThis as unknown as {
  engineState: EngineStateStore | undefined;
};

export const engineState = globalForEngine.engineState ?? new EngineStateStore();

if (process.env.NODE_ENV !== "production") {
  globalForEngine.engineState = engineState;
}
