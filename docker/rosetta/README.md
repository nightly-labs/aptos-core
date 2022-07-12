Rosetta API Dockerfile
====

This directory contains a Dockerfile meant to build a [Rosetta compliant Docker image](https://www.rosetta-api.org/docs/node_deployment.html) of Aptos.

## How to build the image

Use either option

Option 1:
```
docker/rosetta/docker-build-rosetta.sh
```

Option 2:

```
docker buildx build --file docker/rosetta/rosetta.Dockerfile --build-arg=GIT_SHA=<GIT_SHA_YOU_WANT_TO_BUILD> -t aptos-core:rosetta-<GIT_SHA_YOU_WANT_TO_BUILD> -t aptos-core:rosetta-latest .
```

## How to run

The rosetta docker image contains 2 binaries:
- `aptos-node` to run an Aptos FullNode
- `aptos-rosetta` to run a Rosetta compliant API

In production this is meant to be run as 2 seperate containers, where the `aptos-rosetta` container talks to the `aptos-node`.
If you use kubernetes, you would for example run this as 2 containers in the same pod.

We provide a docker-compose example that shows how to do that.

#TODO: docker-compose example run