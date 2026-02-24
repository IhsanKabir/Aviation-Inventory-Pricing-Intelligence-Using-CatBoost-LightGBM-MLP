Cookie files are environment-specific and should not be committed.

Expected files:
- `cookies/biman.json`
- `cookies/novoair.json`

Format:
- Standard browser-export cookie JSON array (`name`, `value`, `domain`, etc.).

Notes:
- Keep cookies refreshed when airline sessions expire.
- If requests start returning auth/challenge pages, refresh cookies and retry.
