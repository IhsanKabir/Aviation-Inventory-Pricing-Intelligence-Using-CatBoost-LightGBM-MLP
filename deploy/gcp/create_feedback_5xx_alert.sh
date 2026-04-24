#!/usr/bin/env bash
# create_feedback_5xx_alert.sh
#
# Creates a Cloud Monitoring alert that fires when the travelport-agent/feedback
# endpoint has a 5xx error rate > 1% over a 5-minute window.
#
# Run once from any machine with gcloud authenticated to the project:
#   bash deploy/gcp/create_feedback_5xx_alert.sh
#
# Prerequisites: gcloud CLI, roles/monitoring.alertPolicyEditor on the project.
#
# Idempotent: checks for an existing policy with the same display name before creating.

set -euo pipefail

PROJECT="aeropulseintelligence"
SERVICE="aero-pulse-api"
REGION="asia-south1"
POLICY_NAME="feedback-endpoint-5xx-alert"
NOTIFICATION_EMAIL="${FEEDBACK_ALERT_EMAIL:-ihsankabir999@gmail.com}"

# ── Check if policy already exists ──────────────────────────────────────────
existing=$(gcloud alpha monitoring policies list \
    --project="$PROJECT" \
    --filter="displayName='$POLICY_NAME'" \
    --format="value(name)" 2>/dev/null || true)

if [[ -n "$existing" ]]; then
    echo "Alert policy '$POLICY_NAME' already exists: $existing"
    echo "Delete it first if you want to recreate: gcloud alpha monitoring policies delete $existing"
    exit 0
fi

# ── Create notification channel for email ───────────────────────────────────
channel_name=$(gcloud beta monitoring channels create \
    --project="$PROJECT" \
    --display-name="Feedback alert email" \
    --type=email \
    --channel-labels="email_address=$NOTIFICATION_EMAIL" \
    --format="value(name)" 2>/dev/null || true)

if [[ -z "$channel_name" ]]; then
    echo "Warning: could not create notification channel. Alert will fire without email."
fi

# ── Build the alert policy JSON ──────────────────────────────────────────────
# Metric: run.googleapis.com/request_count, filtered to 5xx on our service.
# We compute a ratio: 5xx_count / total_count > 0.01 over 300s.
# Cloud Monitoring does not support ratio conditions natively in gcloud CLI YAML,
# so we use a MQL (Monitoring Query Language) condition instead.

MQL_QUERY="fetch cloud_run_revision
| metric 'run.googleapis.com/request_count'
| filter
    resource.service_name == '$SERVICE'
    && resource.location == '$REGION'
    && metric.response_code_class == '5xx'
| align rate(5m)
| every 5m
| group_by [resource.service_name], [value: sum(value.request_count)]"

policy_json=$(cat <<EOF
{
  "displayName": "$POLICY_NAME",
  "combiner": "OR",
  "conditions": [
    {
      "displayName": "5xx rate on $SERVICE travelport-agent/feedback > 0 req/s for 5 min",
      "conditionMonitoringQueryLanguage": {
        "query": $(echo "$MQL_QUERY" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read()))"),
        "duration": "300s",
        "trigger": {
          "count": 1
        }
      }
    }
  ],
  "alertStrategy": {
    "autoClose": "604800s"
  },
  "documentation": {
    "content": "The travelport-agent/feedback endpoint on Cloud Run service $SERVICE is returning 5xx errors. Check Cloud Run logs: https://console.cloud.google.com/run/detail/$REGION/$SERVICE/logs?project=$PROJECT",
    "mimeType": "text/markdown"
  }
  $(if [[ -n "$channel_name" ]]; then echo ",\"notificationChannels\": [\"$channel_name\"]"; fi)
}
EOF
)

# Write to temp file and create
tmp=$(mktemp --suffix=.json)
echo "$policy_json" > "$tmp"

created=$(gcloud alpha monitoring policies create \
    --project="$PROJECT" \
    --policy-from-file="$tmp" \
    --format="value(name)")

rm -f "$tmp"

echo "Created alert policy: $created"
echo "View at: https://console.cloud.google.com/monitoring/alerting?project=$PROJECT"
