<!-- AIINIT:MANAGED:START -->
<!-- Managed by aiinit — regenerated on every run. Do NOT edit, reword, summarize,
     or reorder anything between AIINIT:MANAGED:START and AIINIT:MANAGED:END.
     Put project-specific instructions BELOW the END marker; those are yours to edit. -->

# Project instructions (cloud-safe)

- GitHub is the source of truth.
- Do not push feature work directly to `main`.
- Use a branch for non-trivial work.
- Keep changes focused.
- Run available checks (tests, linters, build) before finishing.
- For non-trivial work: branch -> open a PR -> run `gh pr merge --auto --squash`
  to enable auto-merge YOURSELF. GitHub merges it automatically once CI passes.
  Do NOT manually merge `main` (it bypasses CI), do NOT push feature work straight
  to `main`, and do NOT ask the user to click merge -- auto-merge handles it.
- Never commit secrets.
- Known environment variable names are listed in `docs/global.env.template`.
- If you need real secret values, ask me which variables to configure in the
  cloud environment (Codex Cloud / Claude Code Web). Do not invent or hardcode them.
- When updating project instructions, edit this file (`AGENTS.md`, the universal
  instruction file) — NOT `CLAUDE.md`, which only imports it — so Claude and Codex
  stay in sync.
<!-- AIINIT:MANAGED:END -->

<!-- Project-specific instructions below — edit freely. -->
