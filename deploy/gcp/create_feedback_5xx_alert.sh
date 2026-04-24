#!/usr/bin/env bash
# create_feedback_5xx_alert.sh
#
# Creates a Cloud Monitoring alert that fires when the Cloud Run service
# emits any 5xx responses over a 5-minute window.
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

# ── Build notification channels JSON fragment ────────────────────────────────
if [[ -n "$channel_name" ]]; then
    channels_json="\"notificationChannels\": [\"$channel_name\"],"
else
    channels_json=""
fi

# ── Build the alert policy JSON ──────────────────────────────────────────────
# Uses conditionThreshold (filter + aggregation) instead of MQL so there is
# no requirement to produce an explicit boolean column.
# Fires when the 5xx request rate is > 0 req/s over a 300s alignment window.

tmp=$(mktemp --suffix=.json)
cat > "$tmp" <<EOF
{
  "displayName": "$POLICY_NAME",
  "combiner": "OR",
  $channels_json
  "conditions": [
    {
      "displayName": "5xx responses on $SERVICE > 0 over 5 min",
      "conditionThreshold": {
        "filter": "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = \"$SERVICE\" AND metric.type = \"run.googleapis.com/request_count\" AND metric.labels.response_code_class = \"5xx\"",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_RATE",
            "crossSeriesReducer": "REDUCE_SUM",
            "groupByFields": ["resource.labels.service_name"]
          }
        ],
        "comparison": "COMPARISON_GT",
        "thresholdValue": 0,
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
    "content": "Cloud Run service $SERVICE is returning 5xx errors. Check logs: https://console.cloud.google.com/run/detail/$REGION/$SERVICE/logs?project=$PROJECT",
    "mimeType": "text/markdown"
  }
}
EOF

created=$(gcloud alpha monitoring policies create \
    --project="$PROJECT" \
    --policy-from-file="$tmp" \
    --format="value(name)")

rm -f "$tmp"

echo "Created alert policy: $created"
echo "View at: https://console.cloud.google.com/monitoring/alerting?project=$PROJECT"
