# Pump State Machine

Normalized InteliPump states, events, transitions, guards, and restart
reconciliation are documented in [pump-state-machine.md](pump-state-machine.md).
Command eligibility (evaluation only) is in
[../safety/command-eligibility.md](../safety/command-eligibility.md).

```text
DISCONNECTED
DISCOVERING
NOT_PROGRAMMED
RESET
READY
NOZZLE_UP
AUTHORIZED
FILLING
FILLING_COMPLETE
SUSPENDED
LIMIT_REACHED
FAULTED
MAINTENANCE
```

Rules:

- `DISCONNECTED + AUTHORIZE` is rejected.
- `FILLING + SET_PRICE` is rejected.
- Never automatically authorize after restart.
- Reconcile local state with the physical dispenser after restart.
