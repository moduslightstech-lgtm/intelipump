# Architecture

1. REST, WebSocket/SSE, and MQTT interfaces
2. Controller application services
3. Pump state machine and safety validation
4. Wayne DART application layer
5. Wayne DART line layer
6. Half-duplex RS-485 transport
7. Local persistence
8. Independent watchdog and TX-enable gate

The cloud requests an action. The local controller decides whether it is safe and valid.
