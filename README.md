# kl-site-back

Backend for KL-Law market data website.

Check `config.yaml` for server settings.

## Prerequisites

- Touchance 3.0
- Python 3.10

## Environment Variables

Most of the variables can be used from `kl_site_common.env`.
They should be self-explanatory, and should have default value set to it, unless specified otherwise.

### Required

`FASTAPI_AUTH_SECRET`: must set for authentication.

> Run `openssl rand -hex 32` to generate.

`FASTAPI_AUTH_CALLBACK`: OAuth callback URI. This is used when a user uses the `/auth/token` EP.

> Throws HTTP 400 bad request if the callback URI doesn't match.

`MONGO_URL`: Mongo DB connection string. This should use SRV record (`mongodb+srv://`).
