[Unit]
Description=AirlineIntel 4-hour accumulation timer (09:30, 13:30, 17:30, 21:30)

[Timer]
OnCalendar=*-*-* 09,13,17,21:30:00
Persistent=true
AccuracySec=1min
Unit=airlineintel-ingestion4h.service

[Install]
WantedBy=timers.target

