# parity-cc-routing's local image (phase 1: local-dev and local-prodlike only). The binary is built
# on the host (bazel build //:parity-cc-routing: GCC 15, glibc and libstdc++ of Fedora 43), so the
# runtime is Fedora 43 by digest (the image parity-deploy already pins). parity-deploy's
# build-local-images.sh stages this file with the binary and its sha256.
FROM docker.io/library/fedora@sha256:a651ddf48ea28a06ed4e1e6519f51c9f47e7a5a138722ade87369b8fbb7e5b42
COPY parity-cc-routing /usr/local/bin/parity-cc-routing
# The state dir (the incarnation counter and its lock: the restart rule, CR-059) is a volume; a new
# named volume takes this directory's owner, so the non-root user can write it.
RUN mkdir -p /var/lib/parity-cc-routing && chown 10014:10014 /var/lib/parity-cc-routing && chmod 0700 /var/lib/parity-cc-routing
USER 10014:10014
EXPOSE 9800 9810
ENTRYPOINT ["/usr/local/bin/parity-cc-routing"]
