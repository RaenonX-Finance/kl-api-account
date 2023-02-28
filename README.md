# kl-api-account

Backend for KL account API.

Check `config.yaml` for server settings.

## Prerequisites

- Python 3.10

### Required

`FASTAPI_AUTH_SECRET`: must set for authentication.

> Run `openssl rand -hex 32` to generate.

`FASTAPI_AUTH_CALLBACK`: OAuth callback URI. This is used when a user uses the `/auth/token` EP.

> Throws HTTP 400 bad request if the callback URI doesn't match.

`MONGO_URL`: Mongo DB connection string. This should use SRV record (`mongodb+srv://`).

### Optional

`PATH_CONFIG_BASE`: Base config path. Default is `config.yaml`.

`PATH_CONFIG_OVERRIDE`: Base config path. Default is `config-override.yaml`.

`PATH_CONFIG_SCHEMA`: Base config path. Default is `config.schema.json`.

`DEV`: Set to `1` for enabling development mode.

- API doc is only available under dev mode.
- Log messages will always print to console under dev mode.
