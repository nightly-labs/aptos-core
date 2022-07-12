#!/bin/bash
# Copyright (c) Aptos
# SPDX-License-Identifier: Apache-2.0

# This script docker bake to build all the rust-based docker images
# You need to execute this from the repository root as working directory
# E.g. docker/docker-bake-rust-all.sh
# If you want to build a specific target only, run:
#  docker/docker-bake-rust-all.sh <target>
# E.g. docker/docker-bake-rust-all.sh indexer

set -ex

export GIT_SHA=$(git rev-parse HEAD)

docker buildx build --file docker/rosetta/rosetta.Dockerfile --build-arg=GIT_SHA=$GIT_SHA -t aptos-core:rosetta-$GIT_SHA -t aptos-core:rosetta-latest .
