# transaction-consumer

## Logging and Tracing

This crate now emits events through `tracing`.

Feature flags:

- `log-compat` (default): duplicate events to `log` for existing `env_logger` users.
- `tracing-init`: enables `transaction_consumer::tracing_setup::init_tracing()`.
- `stackdriver`: when used with `tracing-init`, enables stackdriver output on non-TTY.

Notes:

- With `tracing-init + log-compat`, the crate avoids installing `LogTracer` to prevent duplicate events.
- If you need to ingest third-party `log` records into `tracing`, use `default-features = false` and enable `tracing-init`.

Examples:

- Keep legacy logger behavior:
  - `transaction-consumer = "..."`
- Tracing only:
  - `transaction-consumer = { version = \"...\", default-features = false }`
- Tracing init helper:
  - `transaction-consumer = { version = \"...\", features = [\"tracing-init\"] }`
