# EXPERIMENT NOTE — 2026-09-11 (phantom flow / unintended meter climb)
#
# Change: owned-lab no longer auto-AUTHORIZEs on nozzle lift.
# Why: lift alone was enabling Wayne delivery; face climbed without intentional
#      squeeze. Confirmed: AUTHORIZE enables delivery; pump meter can advance
#      without a conscious dispense (hardware once authorized).
#
# Also:
#   - post-RESET stale-_last_dc2 fix (AUTHORIZE not blocked forever after hang-up)
#   - arm-then-lift (preferred UX without sudo authorize while OUT)
#   - LiveFillStream no longer short-settles ACTIVE sales while snap is
#     DISCOVERING (was dropping mid-sale after ~4s DC2 gaps)
#   - DC2 path heals FILLING state + reopens after premature sidecar settle
#
# How to test after deploy:
#   1. Lift only → "AUTHORIZE deferred", face stays 0.00.
#   2. Arm then lift (preferred):
#        sudo touch /var/lib/intelipump/arm-1   # nozzle may be IN
#        lift nozzle → AUTHORIZE once
#   3. Or authorize while OUT:
#        sudo touch /var/lib/intelipump/authorize-1
#   4. After AUTHORIZE, expect live_source_event progress without
#      possible_unintended_flow / DISCOVERING quarantine mid-sale.
#
# REVERT (restore lift → AUTHORIZE):
#   Add --authorize-on-nozzle-lift to systemd ExecStart, daemon-reload, restart.
#
# Related files:
#   src/intelipump_fdc/controller/cli.py
#   src/intelipump_fdc/controller/controller_loop.py
#   src/intelipump_fdc/cloud/fill_stream.py
#   src/intelipump_fdc/services/persistence_bridge.py
#   deploy/systemd/intelipump.service
