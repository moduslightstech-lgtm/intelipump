# API security boundary (Phase 8)

- Bind default `127.0.0.1` (`ApiSettings.host`)
- Trusted-network / LAB only
- No Keycloak / IAM yet
- No production active command endpoints
- Correlation IDs on requests/responses/logs/commands/audit
- Simulator commands require virtual/memory transport proof

Do not expose this API on a public interface without authentication.
