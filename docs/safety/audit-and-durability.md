# Audit and durability (Phase 7)

## Command persistence

Every external or simulator command evaluation that is recorded includes
rejected requests. For rejected AUTHORIZE:

- correlation ID
- command type
- current state
- blocking reasons
- timestamp
- `simulator_only`
- audit record

Active commands are **not executed** in Phase 7.

## Audit hash chain (tamper-evident)

1. Canonicalize record payload (`json.dumps(..., sort_keys=True)`).
2. `record_hash = SHA256(previous_hash + "|" + canonical)`.
3. First record uses genesis `GENESIS_V1`.
4. `AuditRepository.verify_chain()` detects broken links / payload tampering.

### Limitations (explicit)

This is **tamper-evident**, not cryptographic tamper prevention:

- An attacker with DB write access can rebuild the chain.
- No external anchoring, signatures, or HSM.
- Clocks and process crashes do not invalidate the chain by themselves.

## Event durability priorities

| Priority | Examples | Queue-full policy |
|----------|----------|-------------------|
| CRITICAL | transaction complete, rejected commands/audit | refuse drop (`PersistenceQueueFullError`) |
| NORMAL | filling updates, routine state, comm flips | may drop (counted) |

## Persistence worker

- Bounded async priority queue
- Does not block the ~25 ms poll loop
- Flush on clean shutdown
- No unbounded task-per-event creation
