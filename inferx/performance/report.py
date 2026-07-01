# inferx/performance/report.py
import json
from typing import Any, Dict, List


class ReportGenerator:
    """Formats performance benchmarking outputs into Markdown, JSON, and styled HTML dashboard pages."""

    @staticmethod
    def generate_json_report(metrics: Dict[str, Any]) -> str:
        return json.dumps(metrics, indent=2)

    @staticmethod
    def generate_markdown_report(metrics: Dict[str, Any]) -> str:
        """Compiles benchmarking stats into a GitHub markdown table format."""
        lines = [
            "# InferX Performance Benchmark Report",
            "",
            "| Metric Parameter | Measured Value |",
            "| --- | --- |",
            f"| Total Requests | {metrics.get('count', 0)} |",
            f"| Throughput (RPS) | {metrics.get('throughput_rps', 0.0):.2f} req/sec |",
            f"| P50 Latency | {metrics.get('p50', 0.0):.2f} ms |",
            f"| P90 Latency | {metrics.get('p90', 0.0):.2f} ms |",
            f"| P95 Latency | {metrics.get('p95', 0.0):.2f} ms |",
            f"| P99 Latency | {metrics.get('p99', 0.0):.2f} ms |",
            f"| P999 Latency | {metrics.get('p999', 0.0):.2f} ms |",
            f"| CPU Avg Utilization | {metrics.get('cpu_avg', 0.0):.2f}% |",
            f"| Memory Avg Usage | {metrics.get('memory_avg_mb', 0.0):.2f} MB |",
            f"| GPU Avg Utilization | {metrics.get('gpu_avg', 0.0):.2f}% |",
            f"| Avg Batch Size | {metrics.get('batch_size_avg', 0.0):.2f} |",
            f"| Avg Queue Delay | {metrics.get('queue_delay_avg_ms', 0.0):.2f} ms |",
            ""
        ]
        return "\n".join(lines)

    @staticmethod
    def generate_html_report(metrics: Dict[str, Any]) -> str:
        """Compiles stats into a beautiful, styled modern HTML dashboard with gradients."""
        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>InferX Performance Dashboard</title>
    <style>
        body {{
            font-family: 'Outfit', sans-serif;
            background-color: #0d1117;
            color: #c9d1d9;
            margin: 0;
            padding: 40px;
        }}
        .container {{
            max-width: 900px;
            margin: 0 auto;
        }}
        h1 {{
            color: #ffffff;
            font-size: 2.5rem;
            margin-bottom: 5px;
            background: linear-gradient(45deg, #58a6ff, #bc8cff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .subtitle {{
            color: #8b949e;
            margin-bottom: 30px;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        .card {{
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
        }}
        .card .title {{
            font-size: 0.9rem;
            color: #8b949e;
            text-transform: uppercase;
        }}
        .card .value {{
            font-size: 2rem;
            font-weight: bold;
            color: #58a6ff;
            margin-top: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            overflow: hidden;
        }}
        th, td {{
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #30363d;
        }}
        th {{
            background-color: #21262d;
            color: #ffffff;
        }}
        tr:hover {{
            background-color: #21262d;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>InferX Performance Dashboard</h1>
        <div class="subtitle">Cloud-native AI Inference SLA Benchmarks and Recovery Metrics</div>
        
        <div class="grid">
            <div class="card">
                <div class="title">Throughput</div>
                <div class="value">{metrics.get('throughput_rps', 0.0):.1f} RPS</div>
            </div>
            <div class="card">
                <div class="title">P95 Latency</div>
                <div class="value">{metrics.get('p95', 0.0):.2f} ms</div>
            </div>
            <div class="card">
                <div class="title">P99 Latency</div>
                <div class="value">{metrics.get('p99', 0.0):.2f} ms</div>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Benchmark Parameter</th>
                    <th>Measured Metric</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>Total Request Count</td>
                    <td>{metrics.get('count', 0)}</td>
                </tr>
                <tr>
                    <td>P50 Latency (Median)</td>
                    <td>{metrics.get('p50', 0.0):.2f} ms</td>
                </tr>
                <tr>
                    <td>P90 Latency</td>
                    <td>{metrics.get('p90', 0.0):.2f} ms</td>
                </tr>
                <tr>
                    <td>P999 Latency</td>
                    <td>{metrics.get('p999', 0.0):.2f} ms</td>
                </tr>
                <tr>
                    <td>Average CPU Load</td>
                    <td>{metrics.get('cpu_avg', 0.0):.2f}%</td>
                </tr>
                <tr>
                    <td>Average Memory Usage</td>
                    <td>{metrics.get('memory_avg_mb', 0.0):.2f} MB</td>
                </tr>
                <tr>
                    <td>Average GPU Load</td>
                    <td>{metrics.get('gpu_avg', 0.0):.2f}%</td>
                </tr>
                <tr>
                    <td>Average Batch Size</td>
                    <td>{metrics.get('batch_size_avg', 0.0):.2f}</td>
                </tr>
                <tr>
                    <td>Average Queue Scheduling Delay</td>
                    <td>{metrics.get('queue_delay_avg_ms', 0.0):.2f} ms</td>
                </tr>
            </tbody>
        </table>
    </div>
</body>
</html>
"""
        return html_template
