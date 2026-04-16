## Summary

<!-- One or two sentences: what this PR does and why. -->

## Changes

<!-- Bullet list of notable changes. -->

-

## Verification

<!-- Check what you actually ran/clicked. Uncheck and note what you skipped, with a reason. -->

- [ ] `uv run ruff check` — clean
- [ ] `uv run ruff format --check` — clean
- [ ] `uv run mypy` — clean
- [ ] `make dev` up from project root, all services healthy (if runtime affected)
- [ ] Manually exercised the primary flow this change touches
- [ ] Checked `make logs` (this service + related Kafka topics) for new errors

## Linked issues

Closes #
