# DART Application Layer Summary

Implement read-only decoding first:

- pump status
- nozzle status
- live amount and volume
- completed transaction
- prices
- totalizers
- alarms

Define but do not transmit initially:

- reset
- set price
- authorize
- stop
- suspend
- resume
- presets

Do not guess ambiguous fields. Add a TODO and a validation test.
