#!/usr/bin/env bash

# helm repo add cockroachdb https://charts.cockroachdb.com/
helm repo add strimzi https://strimzi.io/charts/

# helm uninstall cockroachdb
helm uninstall strimzi-kafka

helm install --version v0.20.0 strimzi-kafka strimzi/strimzi-kafka-operator
# helm install --version v6.2.1 -f helm-config/cockroach-helm-values.yml cockroachdb cockroachdb/cockroachdb

kubectl apply -f k8s/kafka.yaml
