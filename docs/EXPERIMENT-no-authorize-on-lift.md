# EXPERIMENT NOTE — 2026-09-11 (phantom flow / unintended meter climb)
#
# Change: owned-lab no longer auto-AUTHORIZEs on nozzle lift.
# Why: lift alone was enabling Wayne delivery; face climbed (e.g. 0.46 L)
#      without intentional squeeze. User confirmed those amounts were not
#      real sales they made.
#
# Also kept: post-RESET stale-_last_dc2 fix so AUTHORIZE is not blocked forever
#            after hang-up when a prior face remains cached.
#
# How to test after deploy:
#   1. Lift nozzle only → expect "AUTHORIZE deferred" and face must NOT climb.
#   2. To dispense intentionally (nozzle already OUT):
#        sudo touch /var/lib/intelipump/authorize-1   # addr 1
#        sudo touch /var/lib/intelipump/authorize-2   # addr 2
#   3. If climb still happens with no AUTHORIZE in logs → hardware/other cause.
#
# REVERT (restore lift → AUTHORIZE):
#   A) Preferred: add --authorize-on-nozzle-lift to systemd ExecStart, then
#      sudo systemctl daemon-reload && sudo systemctl restart intelipump
#   B) Or in cli.py set automatic_authorization=owned again (pre-experiment).
#
# Related files:
#   src/intelipump_fdc/controller/cli.py
#   src/intelipump_fdc/controller/controller_loop.py
#   deploy/systemd/intelipump.service
