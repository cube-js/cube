FROM public.ecr.aws/v2l0r3n7/cubejs/cube:v1.2.27-returnUsedPreAgg-amd64 AS builder

FROM node:20.20.2-bookworm-slim

ARG IMAGE_VERSION=v1.2.27-hotfix.0

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=prod

RUN DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends libssl3 python3.11 libpython3.11-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN yarn policies set-version v1.22.22

ENV NODE_ENV=production

WORKDIR /cubejs

COPY --from=builder /cubejs .

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH=/cube/conf/node_modules:/cube/node_modules
ENV PYTHONUNBUFFERED=1
RUN ln -s /cubejs/packages/cubejs-docker /cube
RUN ln -s /cubejs/rust/cubestore/bin/cubestore-dev /usr/local/bin/cubestore-dev
COPY --from=builder /usr/local/bin/cubejs /usr/local/bin/cubejs

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
