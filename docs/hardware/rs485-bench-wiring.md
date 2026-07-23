# RS-485 Office Bench Wiring (Phase 10)

LAB office bench only. **No hazardous-location or dispenser wiring instructions.**

## Topology

```
Raspberry Pi
  USB ── Adapter A (controller)
  USB ── Adapter B (simulator)

Adapter A signal pair ←→ Adapter B signal pair
```

## Rules

1. Use **two isolated USB-RS485 adapters**.
2. Adapter A and adapter B connect **only to one another**.
3. Initially connect **A-to-A and B-to-B** (A+/A− to B+/B− per vendor labels).
4. Vendor A/B silk-screen labels may be reversed between brands.
5. If no communication is observed after a safe software shutdown, swap A/B
   **only after** stopping controller and simulator processes.
6. **Never** connect adapter power outputs together or to dispenser power.
7. Connect signal ground **only when required by both adapter manuals**.
8. Short bench wiring normally does **not** require termination.
9. Record termination and bias switch positions in the bench report.
10. Avoid multiple active bias networks on the short bench segment.
11. **No Wayne dispenser connection in Phase 10.**

## Suggested switch defaults (short cable)

| Item | Default |
| --- | --- |
| Termination | OFF |
| Bias | OFF |
| Auto direction | ON (adapter hardware) |

## Safety

- Environment: LAB
- Mode: LISTEN_ONLY
- Active commands disabled
- No field auto-enable
