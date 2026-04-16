# Sentinel + Apalis Redis

This example covers the basics of setting up redis-sentinel and apalis-redis offering high availability.

## Getting started

> Setup your sentinel infra. In this example we will follow the example provided [here](https://medium.com/@mohsenmahoski/setting-up-sentinel-with-docker-compose-5cad962c7643)


### Running locally

Create a .env file with your host IP:

```env
HOST_IP=
```

I used this command:

```bash
ipconfig getifaddr en0
```

Run docker compose:

```bash
docker-compose --env-file .env up
```

Now start the worker

```bash
SENTINEL_NODES=redis://127.0.0.1:26379,redis://127.0.0.1:26380,redis://127.0.0.1:26381 cargo run cargo run --example sentinel
```
