# Cluster add-ons, installed with Helm:
#   kube-prometheus-stack   Prometheus, Alertmanager, Grafana, and the PodMonitor/PrometheusRule CRDs
#   prometheus-pushgateway  where the drift CronJob pushes its gauges
#   argo-rollouts           blue-green and canary rollouts with automated analysis
#   fluent-bit              ships prediction log lines from pod stdout to S3

resource "helm_release" "kube_prometheus_stack" {
  name             = "kube-prometheus-stack"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = var.chart_versions["kube_prometheus_stack"]
  namespace        = "monitoring"
  create_namespace = true

  values = [yamlencode({
    prometheus = {
      prometheusSpec = {
        # Pick up PodMonitors and PrometheusRules from every namespace, not just this release's
        podMonitorSelectorNilUsesHelmValues     = false
        serviceMonitorSelectorNilUsesHelmValues = false
        ruleSelectorNilUsesHelmValues           = false
        retention                               = "15d"
      }
    }
    grafana = {
      sidecar = {
        dashboards = {
          enabled         = true
          label           = "grafana_dashboard"
          searchNamespace = "ALL"
        }
      }
    }
  })]

  depends_on = [module.eks]
}

resource "helm_release" "pushgateway" {
  name       = "prometheus-pushgateway"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "prometheus-pushgateway"
  version    = var.chart_versions["prometheus_pushgateway"]
  namespace  = "monitoring"

  values = [yamlencode({
    serviceMonitor = {
      enabled = true
      # Keep the drift job's labels (model_version) instead of the pushgateway's
      honorLabels = true
    }
  })]

  depends_on = [helm_release.kube_prometheus_stack]
}

resource "helm_release" "argo_rollouts" {
  name             = "argo-rollouts"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-rollouts"
  version          = var.chart_versions["argo_rollouts"]
  namespace        = "argo-rollouts"
  create_namespace = true

  values = [yamlencode({
    dashboard = { enabled = true }
  })]

  depends_on = [module.eks]
}

resource "helm_release" "fluent_bit" {
  name             = "fluent-bit"
  repository       = "https://fluent.github.io/helm-charts"
  chart            = "fluent-bit"
  version          = var.chart_versions["fluent_bit"]
  namespace        = "logging"
  create_namespace = true

  values = [templatefile("${path.module}/fluent-bit-values.yaml.tftpl", {
    role_arn      = module.log_shipper_role.role_arn
    bucket        = module.artifacts.bucket_name
    region        = var.region
    app_namespace = var.app_namespace
  })]

  depends_on = [module.eks]
}
