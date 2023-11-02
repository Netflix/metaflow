#!/bin/bash -e

protos=(
k8s.io/api/core/v1/generated.proto
k8s.io/apimachinery/pkg/api/resource/generated.proto
k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto
k8s.io/apimachinery/pkg/runtime/generated.proto
k8s.io/apimachinery/pkg/runtime/schema/generated.proto
k8s.io/apimachinery/pkg/util/intstr/generated.proto
)

for proto in "${protos[@]}"; do
    url=$(echo "$proto" | sed -E -e 's|k8s.io/([^/]+)/(.*)|https://raw.githubusercontent.com/kubernetes/\1/master/\2|')
    mkdir -p "$(dirname "$proto")"
    wget --quiet -O "$proto" "$url"
done



protoc components.proto --python_out . -I .
