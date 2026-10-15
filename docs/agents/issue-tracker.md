# Issue Tracker: Linear

Issues for this repo are tracked in **Linear** via the `ACE_linear` MCP tools.

## Project Details

- **Workspace / Team**: `StarSrc` (`STARSRC`, ID: `8cc87de1-eb63-4863-ba60-1c701163997f`)
- **Project**: `Bowl` (`P-STARSRC-2`, UUID: `4f644357-8dfa-4df8-9c11-7c0ae127e808`)

## Agent Operations

When engineering skills (`to-tickets`, `triage`, `implement`, `wayfinder`, etc.) interact with issues:

- **Create issues**: Call `ACE_linear:save_issue` specifying `team: "StarSrc"`, `project: "4f644357-8dfa-4df8-9c11-7c0ae127e808"`, and appropriate labels / title / description.
- **Find issues**: Call `ACE_linear:list_issues` or `ACE_linear:get_issue`.
- **Apply triage labels**: Use label names or IDs documented in `docs/agents/triage-labels.md`.
- **Update comments / status**: Call `ACE_linear:save_comment` or update issue status with `ACE_linear:save_issue`.
