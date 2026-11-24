<# Removes everything TaskForge created (namespace, pods, secrets, Redis data). Run: .\deploy\down.ps1 #>
kubectl delete namespace taskforge
