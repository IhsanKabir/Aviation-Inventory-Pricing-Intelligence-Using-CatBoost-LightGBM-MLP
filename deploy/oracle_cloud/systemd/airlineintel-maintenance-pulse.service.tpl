[Unit]
Description=AirlineIntel maintenance pulse (recovery + due ops checks)
Wants=network-online.target postgresql.service
After=network-online.target postgresql.service

[Service]
Type=oneshot
User=__APP_USER__
Group=__APP_GROUP__
WorkingDirectory=__APP_DIR__
Environment=PYTHONUNBUFFERED=1
ExecStart=/bin/bash __APP_DIR__/scheduler/run_maintenance_pulse_once.sh
Nice=10

