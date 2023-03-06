# kl-api-account

KL account API.

Check `config.yaml` for server settings.

## Prerequisites

- Python 3.10

### Environment setup

#### `./.env`

Check `./kl_api_common/env.py` for all possible variables.

(Required) `APP_NAME`: App name. This is used for as log file name.

(Required) `FASTAPI_AUTH_SECRET`: For authentication.

> Run `openssl rand -hex 32` to generate.

(Required) `FASTAPI_AUTH_CALLBACK`: OAuth callback URI. This is used when a user uses the `/auth/token` EP.

> Throws HTTP 400 bad request if the callback URI doesn't match.

(Required) `MONGO_URL`: Mongo DB connection string. This should be SRV record (`mongodb+srv://`).

(Required) `NEW_RELIC_LICENSE_KEY`: New Relic license key.

(PROD Only) `NEW_RELIC_APP_NAME`: New Relic app name.

(Optional) `PATH_CONFIG_BASE`: Base config path. Default is `config.yaml`.

(Optional) `PATH_CONFIG_OVERRIDE`: Base config path. Default is `config-override.yaml`.

(Optional) `PATH_CONFIG_SCHEMA`: Base config path. Default is `config.schema.json`.

(Optional) `DEV`: Set to `1` for enabling development mode.

> API doc is only available under dev mode. Log messages will always print to console under dev mode.

#### `./config-override.yml`

Config overriding file. This should follow the same schema as `config.yaml`, all fields are optional. 