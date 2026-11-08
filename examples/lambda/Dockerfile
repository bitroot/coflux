# A worker image for a Lambda pool. `scripts/deploy-pool.py` builds it
# (see deploy.toml); by hand, from this directory, with the CLI built for
# linux on the function's architecture:
#
#   (cd ../../cli && GOOS=linux GOARCH=amd64 go build \
#     -ldflags "-X github.com/bitroot/coflux/cli/internal/version.Version=$(cat ../VERSION)" \
#     -o ../examples/lambda/.build/coflux ./cmd/coflux)
#   docker build --platform linux/amd64 --provenance=false \
#     --build-context coflux=../../adapters/python/coflux \
#     --build-context cli=.build \
#     -t coflux-lambda-demo .
#
# Outside this repo, install the adapter with pip and the CLI from a
# release instead, at the same version as each other.
FROM public.ecr.aws/lambda/python:3.12

COPY --from=cli coflux /usr/local/bin/coflux
COPY --from=coflux . ${LAMBDA_TASK_ROOT}/coflux/

COPY handler.py lambda_demo.py ${LAMBDA_TASK_ROOT}/

# The adapter command, so the worker doesn't go looking for a virtualenv,
# and a writable home: /tmp is the only writable path in Lambda. The
# connection details come with each invocation.
ENV COFLUX_WORKER_ADAPTER=python,-m,coflux \
    HOME=/tmp \
    PYTHONUNBUFFERED=1

CMD ["handler.handler"]
