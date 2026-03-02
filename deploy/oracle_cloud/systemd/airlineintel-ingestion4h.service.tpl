[Unit]
Description=AirlineIntel 4-hour accumulation cycle
Wants=network-online.target postgresql.service
After=network-online.target postgresql.service

[Service]
Type=oneshot
User=__APP_USER__
Group=__APP_GROUP__
WorkingDirectory=__APP_DIR__
Environment=PYTHONUNBUFFERED=1
ExecStart=/bin/bash __APP_DIR__/scheduler/run_ingestion_4h_once.sh
Nice=10

