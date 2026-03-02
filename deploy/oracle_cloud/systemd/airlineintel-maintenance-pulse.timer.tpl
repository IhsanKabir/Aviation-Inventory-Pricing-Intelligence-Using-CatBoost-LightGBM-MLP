[Unit]
Description=AirlineIntel maintenance pulse timer (every 30 minutes)

[Timer]
OnBootSec=5min
OnUnitActiveSec=30min
Persistent=true
AccuracySec=1min
Unit=airlineintel-maintenance-pulse.service

[Install]
WantedBy=timers.target

