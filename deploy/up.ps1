<#
  Deploys TaskForge + Redis + Prometheus + Grafana to the local Docker Desktop Kubernetes cluster.
  Run from PowerShell:   .\deploy\up.ps1
  Safe to re-run: existing secrets are kept, manifests are re-applied.
#>
param([switch]$AnyContext)

# Native commands write progress to stderr; check $LASTEXITCODE explicitly instead of throwing.
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ns = "taskforge"

function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }

function New-RandomSecret {
    $bytes = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    return ([Convert]::ToBase64String($bytes)).TrimEnd("=").Replace("+", "-").Replace("/", "_")
}

# Guard: never deploy to a real cluster by accident.
$ctx = (kubectl config current-context)
if ($LASTEXITCODE -ne 0) { Fail "kubectl cannot find a cluster. Is Kubernetes enabled in Docker Desktop?" }
if ($ctx -ne "docker-desktop" -and -not $AnyContext) {
    Fail "Current kubectl context is '$ctx', not 'docker-desktop'. Re-run with -AnyContext if that is intended."
}
Write-Host "Using context: $ctx" -ForegroundColor Cyan

kubectl apply -f "$root\k8s\namespace.yaml" | Out-Null
if ($LASTEXITCODE -ne 0) { Fail "could not create namespace" }

# Secrets are generated here and never written to disk or git.
kubectl -n $ns get secret taskforge-secrets *> $null
if ($LASTEXITCODE -ne 0) {
    $apiKey = New-RandomSecret
    kubectl -n $ns create secret generic taskforge-secrets --from-literal=TF_API_KEYS=$apiKey | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "could not create taskforge-secrets" }
    Write-Host "`nYour TaskForge API key (shown once, save it): $apiKey" -ForegroundColor Yellow
} else {
    Write-Host "taskforge-secrets already exists, keeping it."
}

kubectl -n $ns get secret grafana-admin *> $null
if ($LASTEXITCODE -ne 0) {
    $pw = New-RandomSecret
    kubectl -n $ns create secret generic grafana-admin --from-literal=password=$pw | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "could not create grafana-admin" }
    Write-Host "Grafana login: user 'admin', password: $pw" -ForegroundColor Yellow
} else {
    Write-Host "grafana-admin already exists, keeping it."
}

# metrics-server is required for the HorizontalPodAutoscalers. kind needs --kubelet-insecure-tls.
kubectl -n kube-system get deployment metrics-server *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing metrics-server..." -ForegroundColor Cyan
    kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "could not install metrics-server (no internet?)" }
    kubectl -n kube-system patch deployment metrics-server --type=json --patch-file "$root\metrics-server-patch.json" | Out-Null
}

Write-Host "Applying manifests..." -ForegroundColor Cyan
kubectl apply -k "$root\k8s"
if ($LASTEXITCODE -ne 0) { Fail "kubectl apply failed (see error above)" }

Write-Host "`nWaiting for everything to become ready (first run pulls images, can take a few minutes)..." -ForegroundColor Cyan
foreach ($w in @("statefulset/redis", "deployment/taskforge-api", "deployment/taskforge-worker", "deployment/prometheus", "deployment/grafana")) {
    kubectl -n $ns rollout status $w --timeout=300s
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`n$w did not become ready. Diagnose with:" -ForegroundColor Red
        Write-Host "  kubectl -n $ns get pods"
        Write-Host "  kubectl -n $ns describe pod <pod-name>"
        Write-Host "  kubectl -n $ns logs <pod-name>"
        exit 1
    }
}

Write-Host "`nAll up. Open these (each in its own PowerShell window, leave it running):" -ForegroundColor Green
Write-Host "  kubectl -n $ns port-forward svc/taskforge-api 8000:80     -> http://localhost:8000/docs"
Write-Host "  kubectl -n $ns port-forward svc/grafana 3000:3000         -> http://localhost:3000"
Write-Host "  kubectl -n $ns port-forward svc/prometheus 9090:9090      -> http://localhost:9090"
