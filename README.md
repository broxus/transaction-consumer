# transaction-consumer

## Logging and Tracing

This crate now emits events through `tracing`.

Feature flags:

- `log-compat` (default): enables `tracing` -> `log` compatibility (`tracing/log`) for existing `env_logger` users.

Notes:

- Tracing subscriber initialization belongs to the application (binary), not this library.

Examples:

- Keep legacy logger behavior:
  - `transaction-consumer = "..."`
- Tracing only:
  - `transaction-consumer = { version = \"...\", default-features = false }`
