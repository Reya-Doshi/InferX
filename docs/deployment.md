# Deployment Guide - InferX

This guide describes how to configure packaging, Docker Compose, and Kubernetes Helm chart deployments.

---

## 1. Local Container Deployment

Build the multi-stage Docker image from the repository root:
```bash
docker build -t inferx/runtime:latest -f deploy/docker/Dockerfile .
```

Spin up the local multi-node cluster (Gateway, Coordinator, 2 Workers):
```bash
docker-compose -f deploy/docker/docker-compose.yml up -d
```

---

## 2. Kubernetes Cluster Deployment

Deploy InferX into your Kubernetes cluster using the parameterized Helm chart:
```bash
helm install inferx deploy/helm/inferx/ -n inferx --create-namespace
```

### Parameterizing GPU Support
Update `deploy/helm/inferx/values.yaml` or pass parameters to bind worker pods to NVIDIA GPU hardware resources:
```bash
helm install inferx deploy/helm/inferx/ \
  --set gpu.enabled=true \
  --set gpu.resources."nvidia\.com/gpu"=1 \
  -n inferx
```

### Custom Metric Autoscaler (HPA)
InferX triggers scale-out actions based on custom queue depth metrics. Verify the HPA configurations using:
```bash
kubectl get hpa -n inferx
```

---

## 3. Gemini Provider local Setup & Configuration

InferX integrates a production-ready `GeminiProvider` using the official `google-genai` Python SDK.

### Environment Variables
Configure the following keys in a `.env` file at the root of the project:

```env
# Google Gemini API key used for predictions
GEMINI_API_KEY=your_gemini_api_key_here

# Gateway API token used to authenticate client requests
INFERX_AUTH_TOKEN=super-secret-auth-token-123
```

### Local Setup
1. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   pip install google-genai python-dotenv
   ```
2. Start the local server:
   ```bash
   python -m inferx.main
   ```
3. Queries submitted to `POST /predict` (or via the interactive playground at `http://127.0.0.1:8000/`) will automatically route through `GeminiProvider` to return real AI completions.
