# Build the manager binary
FROM --platform=$BUILDPLATFORM golang:1.25.4-trixie AS builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    go mod download

# Copy the go source
COPY api/ api/
COPY internal/ internal/
COPY cmd/ cmd/
COPY hack/ hack/

ARG TARGETOS
ARG TARGETARCH
ARG BUILDPLATFORM
ARG LDFLAGS
ENV BUILDARCH=${BUILDPLATFORM##*/}

FROM builder AS ceph-bucket-provider-builder
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GO111MODULE=on go build -ldflags="${LDFLAGS}" -a -o bin/ceph-bucket-provider ./cmd/bucketprovider/main.go

# Start from Kubernetes Debian base.
FROM builder AS ceph-volume-provider-builder
# Install necessary dependencies

RUN apt update && apt install -y libcephfs-dev librbd-dev librados-dev libc-bin ca-certificates

# Build
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    if [ "$TARGETARCH" != "$BUILDARCH" ] && [ "$TARGETARCH" = "arm64" ]; then \
      export CC="/usr/bin/aarch64-linux-gnu-gcc"; \
      export CGO_LDFLAGS="-L/usr/lib/aarch64-linux-gnu -Wl,-lrados -Wl,-lrbd"; \
    elif [ "$TARGETARCH" != "$BUILDARCH" ] && [ "$TARGETARCH" = "amd64" ]; then \
      export CC="/usr/bin/x86_64-linux-gnu-gcc"; \
      export CGO_LDFLAGS="-L/usr/lib/x86_64-linux-gnu -Wl,-lrados -Wl,-lrbd"; \
    else \
      export CC="/usr/bin/gcc"; \
      export CGO_LDFLAGS=""; \
    fi && \
    CGO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    CC="$CC" CGO_LDFLAGS="$CGO_LDFLAGS" GO111MODULE=on \
    go build -ldflags="${LDFLAGS} -linkmode=external" -a -o bin/ceph-volume-provider ./cmd/volumeprovider/main.go

FROM debian:trixie-slim AS ceph-volume-provider-image
ENV LIB_DIR_PREFIX=x86_64
ENV LIB_DIR_PREFIX_MINUS=x86-64
WORKDIR /
COPY --from=ceph-volume-provider-builder /lib/${LIB_DIR_PREFIX}-linux-gnu /lib/${LIB_DIR_PREFIX}-linux-gnu
RUN mkdir -p /lib64
COPY --from=ceph-volume-provider-builder /lib64/ld-linux-${LIB_DIR_PREFIX_MINUS}.so.2 /lib64/
RUN mkdir -p /usr/lib/${LIB_DIR_PREFIX}-linux-gnu/ceph/
COPY --from=ceph-volume-provider-builder /usr/lib/${LIB_DIR_PREFIX}-linux-gnu/ /usr/lib/${LIB_DIR_PREFIX}-linux-gnu/
COPY --from=ceph-volume-provider-builder /etc/ssl/certs /etc/ssl/certs

COPY --from=ceph-volume-provider-builder /workspace/bin/ceph-volume-provider /ceph-volume-provider

# Build stage used for validation of the output-image
# See validate-container-linux-* targets in Makefile
FROM ceph-volume-provider-image AS validation-image

COPY --from=builder /workspace/hack/print-missing-deps.sh /print-missing-deps.sh
SHELL ["/bin/bash", "-c"]
RUN /print-missing-deps.sh


# Final build stage, create the real Docker image with ENTRYPOINT
FROM ceph-volume-provider-image AS ceph-volume-provider
USER 65532:65532

ENTRYPOINT ["/ceph-volume-provider"]

FROM debian:trixie-slim AS ceph-bucket-provider
COPY --from=ceph-bucket-provider-builder /workspace/bin/ceph-bucket-provider /ceph-bucket-provider
USER 65532:65532
ENTRYPOINT ["/ceph-bucket-provider"]
