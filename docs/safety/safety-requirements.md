# Safety Requirements

- Default mode is `LISTEN_ONLY`.
- Active commands are disabled by default.
- Remote authorization is disabled by default.
- Require software configuration and a physical safety-enable signal for active commands.
- Never automatically authorize after restart.
- Never blindly retry non-idempotent commands.
- Audit every external command.
- Preserve raw frames.
- Bench-test active commands only on disabled or controlled equipment.
