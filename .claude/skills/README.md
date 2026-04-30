# Project Skills — Aero Pulse

Project-scoped Claude Code skills for the Aero Pulse Intelligence Platform. Two groups:

1. **Aero-extraction skills** — written for this project, document the data extraction strategy
2. **ECC curated subset** — sourced from [affaan-m/everything-claude-code](https://github.com/affaan-m/everything-claude-code) (MIT),
   copied here for project-pinned use. The full ECC bundle lives at user level (`~/.claude/skills/`) and applies to all projects.

These extend the global skills under `~/.claude/skills/`.

## Aero Pulse extraction skills

Read in this order when working on data extraction.

| Skill | Purpose |
|-------|---------|
| [aero-extraction-overview](aero-extraction-overview/SKILL.md) | Master reference — taxonomy, dispatcher contract, fallback semantics, decision tree |
| [aero-extraction-direct-api](aero-extraction-direct-api/SKILL.md) | BG / VQ / AK / 6E direct connectors |
| [aero-extraction-capture-replay](aero-extraction-capture-replay/SKILL.md) | G9 / OV / Q2 HAR + Playwright capture lifecycle |
| [aero-extraction-ota-fallback](aero-extraction-ota-fallback/SKILL.md) | BS / 2A wrappers + GoZayaan / BDFare / AMYBD / ShareTrip |
| [aero-extraction-sessions-preflight](aero-extraction-sessions-preflight/SKILL.md) | Session refresh, pre-flight gate, extraction-health gate |
| [aero-extraction-hardening](aero-extraction-hardening/SKILL.md) | Tier 1-3 strengthening recommendations |

## ECC curated subset

Skills from the ECC bundle pinned to this repo because they directly support Aero Pulse's stack.

### Domain (scraping / connectors)

| Skill | When to use |
|-------|-------------|
| [data-scraper-agent](data-scraper-agent/SKILL.md) | Designing or hardening an automated data collection agent |
| [api-connector-builder](api-connector-builder/SKILL.md) | Adding a new airline / OTA module — match existing patterns |
| [regex-vs-llm-structured-text](regex-vs-llm-structured-text/SKILL.md) | Deciding parser strategy for fare-rule / penalty text |

### Database / persistence

| Skill | When to use |
|-------|-------------|
| [postgres-patterns](postgres-patterns/SKILL.md) | Query optimization, indexing, schema design |
| [database-migrations](database-migrations/SKILL.md) | Alembic migrations, schema changes, rollback safety |

### ML / forecasting

| Skill | When to use |
|-------|-------------|
| [pytorch-patterns](pytorch-patterns/SKILL.md) | Working in `predict_next_day.py` and ML core |
| [cost-aware-llm-pipeline](cost-aware-llm-pipeline/SKILL.md) | Zero-budget operating constraint |

### Web (apps/web)

| Skill | When to use |
|-------|-------------|
| [nextjs-turbopack](nextjs-turbopack/SKILL.md) | Next.js 16+ build / dev speed |

### Project hygiene

| Skill | When to use |
|-------|-------------|
| [architecture-decision-records](architecture-decision-records/SKILL.md) | Companion to `PROJECT_DECISIONS.md` |
| [repo-scan](repo-scan/SKILL.md) | Source asset audit, secret scan |
| [codebase-onboarding](codebase-onboarding/SKILL.md) | Onboarding flow / new contributor briefs |
| [documentation-lookup](documentation-lookup/SKILL.md) | Up-to-date library docs via Context7 |
| [git-workflow](git-workflow/SKILL.md) | Branching, commits, merge/rebase |
| [github-ops](github-ops/SKILL.md) | Issues, PRs, CI |
| [terminal-ops](terminal-ops/SKILL.md) | Evidence-first repo execution |

### Security

| Skill | When to use |
|-------|-------------|
| [security-review](security-review/SKILL.md) | Auth, user input, secrets, crypto changes |
| [security-scan](security-scan/SKILL.md) | Scan `.claude/` config for vulnerabilities |

### Research / context

| Skill | When to use |
|-------|-------------|
| [deep-research](deep-research/SKILL.md) | Multi-source web research before coding |
| [search-first](search-first/SKILL.md) | Find existing libraries/tools before building |
| [exa-search](exa-search/SKILL.md) | Neural search for web/code/company research |
| [prompt-optimizer](prompt-optimizer/SKILL.md) | Tighten agent prompts |
| [rules-distill](rules-distill/SKILL.md) | Extract cross-cutting principles to rules |
| [context-budget](context-budget/SKILL.md) | Audit context window consumption |
| [token-budget-advisor](token-budget-advisor/SKILL.md) | Track / control token spend |

## Authority and override

- Aero-extraction skills override generic `backend-patterns` / `python-patterns` / `api-design` for anything related to airline data extraction.
- Curated ECC skills here override the user-level versions of the same name (Claude Code resolves project-scope first).
- Generic skills still apply to non-extraction code (FastAPI app, prediction pipeline, web UI).

## Updating the curated subset

These were copied from `affaan-m/everything-claude-code/skills/<name>/SKILL.md` on the ECC main branch. To refresh:

```bash
git clone --depth=1 https://github.com/affaan-m/everything-claude-code /tmp/ecc-clone
# copy the skills you want; keep the directory layout flat (one SKILL.md per folder)
```

Don't edit the ECC-sourced files in place — if you need project-specific guidance, write a new `aero-*` skill and reference the ECC one.
