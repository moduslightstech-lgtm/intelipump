# Owned-lab AUTHORIZE policy (updated 2026-09-11)
#
# Default (production-like): AUTHORIZE on nozzle lift with
# --confirm-owned-lab-dispense-session. No extra touch/arm command needed.
#
# Lab finding: once AUTHORIZE is sent, Wayne can advance the meter without a
# conscious squeeze (hardware). Software cannot stop that after AUTHORIZE.
# Keeping lift→AUTHORIZE matches intended site UX; treat post-auth climb as
# pump/hardware behavior to investigate separately.
#
# Opt out (arm-only lab mode):
#   Add --no-authorize-on-nozzle-lift to systemd ExecStart, then:
#     sudo touch /var/lib/intelipump/arm-1   # then lift
#     # or authorize-<addr> while nozzle OUT
#
# Related hardening kept regardless of auto-lift:
#   - post-RESET stale-_last_dc2 must not block next AUTHORIZE
#   - LiveFillStream must not short-settle ACTIVE sales while DISCOVERING
#   - DC2 heals FILLING + reopens after premature sidecar settle
#
# Related files:
#   src/intelipump_fdc/controller/cli.py
#   src/intelipump_fdc/controller/controller_loop.py
#   src/intelipump_fdc/cloud/fill_stream.py
#   src/intelipump_fdc/services/persistence_bridge.py
#   deploy/systemd/intelipump.service
