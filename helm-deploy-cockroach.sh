#!/usr/bin/env bash

helm repo add cockroachdb https://charts.cockroachdb.com/

helm uninstall cockroachdb

helm install  --version v6.2.1 -f helm-config/cockroach-helm-values.yml cockroachdb cockroachdb/cockroachdb
