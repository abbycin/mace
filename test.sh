#!/usr/bin/env bash

cargo test $1 && cargo test --test gc $1 --features="test_disable_recycle"