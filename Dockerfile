FROM ghcr.io/calagopus/kopia:latest AS kopia
FROM alpine:latest

RUN apk add --no-cache ca-certificates coreutils curl btrfs-progs xfsprogs-extra zfs restic nftables conntrack-tools && \
	update-ca-certificates
RUN rm -rf /var/lib/apt/lists/*

COPY --from=kopia /usr/local/bin/kopia /usr/bin/kopia

# Add calagopus-wings and entrypoint
ARG TARGETPLATFORM
ARG WINGS_VERSION=unknown
COPY .docker/${TARGETPLATFORM#linux/}/calagopus-wings /usr/bin/calagopus-wings

ENV OCI_CONTAINER=official
LABEL org.opencontainers.image.version=$WINGS_VERSION

ENTRYPOINT ["/usr/bin/calagopus-wings"]
