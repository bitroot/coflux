FROM node:20 AS npm_build
WORKDIR /app

COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

# COPY frontend/esbuild.js frontend/tailwind.config.js frontend/tsconfig.json ./
# COPY frontend/src src/
COPY frontend/ ./
RUN npm run build


FROM elixir:1.15.7 AS mix_build
WORKDIR /app

RUN mix local.hex --force && \
    mix local.rebar --force

COPY server/mix.exs server/mix.lock server/VERSION ./
RUN mix deps.get --only prod

# COPY server/lib lib/
# COPY server/priv priv/
COPY server/ ./
COPY --from=npm_build /app/out/ priv/static/

RUN MIX_ENV=prod mix release


FROM debian:bookworm-20240211
WORKDIR /app

ARG data_dir=/data
ENV COFLUX_DATA_DIR=${data_dir}
RUN mkdir ${data_dir}

RUN apt-get update -y && apt-get install -y libstdc++6 openssl libncurses5 locales \
    && apt-get clean && rm -f /var/lib/apt/lists/*_*

RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

COPY --from=mix_build /app/_build/prod/rel/coflux ./

CMD [ "/app/bin/coflux", "start" ]
