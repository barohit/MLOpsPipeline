class PrometheusGrafanaMonitoringService:
    def get_pod_annotations(self, metrics_path: str, metrics_port: int) -> dict[str, str]:
        return {
            "prometheus.io/scrape": "true",
            "prometheus.io/path": metrics_path,
            "prometheus.io/port": str(metrics_port),
        }

    def build_grafana_dashboard_configmap(self, model_name: str, namespace: str) -> dict:
        dashboard_name = f"{model_name.lower().replace('_', '-')}-dashboard"

        dashboard_json = {
            "title": f"{model_name} Dashboard",
            "timezone": "browser",
            "schemaVersion": 39,
            "version": 1,
            "refresh": "5s",
            "panels": [
                {
                    "title": "Prediction Request Rate",
                    "type": "timeseries",
                    "targets": [
                        {
                            "expr": "rate(team_win_prediction_requests_total[5m])",
                            "legendFormat": "requests/sec",
                        }
                    ],
                    "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8},
                },
                {
                    "title": "Prediction Error Rate",
                    "type": "timeseries",
                    "targets": [
                        {
                            "expr": "rate(team_win_prediction_errors_total[5m])",
                            "legendFormat": "errors/sec",
                        }
                    ],
                    "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8},
                },
                {
                    "title": "p50 / p95 Latency",
                    "type": "timeseries",
                    "targets": [
                        {
                            "expr": "histogram_quantile(0.50, sum(rate(team_win_prediction_request_latency_seconds_bucket[5m])) by (le))",
                            "legendFormat": "p50",
                        },
                        {
                            "expr": "histogram_quantile(0.95, sum(rate(team_win_prediction_request_latency_seconds_bucket[5m])) by (le))",
                            "legendFormat": "p95",
                        },
                    ],
                    "gridPos": {"x": 0, "y": 8, "w": 24, "h": 8},
                },
            ],
        }

        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": dashboard_name,
                "namespace": namespace,
                "labels": {
                    "grafana_dashboard": "1",
                },
            },
            "data": {
                f"{dashboard_name}.json": __import__("json").dumps(dashboard_json)
            },
        }