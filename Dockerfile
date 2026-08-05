# Build both runnable distributions, then run on a slim JRE. One image carries
# the pair — same code, same store lib, same schema — so the relay and the sync
# process cannot drift apart by tag; docker-compose picks the process with an
# `entrypoint` override on the sync service.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
# The cache mount keeps Gradle's dependency cache across builds — COPY . .
# invalidates this layer on every commit, and the JitPack-built quartz and
# vespa-eventstore artifacts are slow to re-download from scratch.
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon :relay:installDist :sync:installDist

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/relay/build/install/vespa-relay /app/relay
COPY --from=build /src/sync/build/install/vespa-sync /app/sync
# The NIP-50 websocket + NIP-11 + web UI (RELAY_PORT, default 7777).
EXPOSE 7777
# The serving relay is the default; /app/sync/bin/vespa-sync is the mirror.
ENTRYPOINT ["/app/relay/bin/vespa-relay"]
