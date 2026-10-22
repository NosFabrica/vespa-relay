# Build both runnable distributions, then run on a slim JRE. One image carries
# the pair — same code, same store lib, same schema — so the relay and the sync
# process cannot drift apart by tag; docker-compose picks the process with an
# `entrypoint` override on the sync service.
FROM eclipse-temurin:21.0.12.1_1-jdk-noble AS build
WORKDIR /src
COPY . .
# The cache mount keeps Gradle's dependency cache across builds — COPY . .
# invalidates this layer on every commit, and the JitPack-built quartz and
# vespa-eventstore artifacts are slow to re-download from scratch.
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon :relay:installDist :sync:installDist

FROM eclipse-temurin:21.0.12.1_1-jre-noble
WORKDIR /app
COPY --from=build /src/relay/build/install/vespa-relay /app/relay
COPY --from=build /src/sync/build/install/vespa-sync /app/sync
# A class-data archive per process (AppCDS), built here from the classes a real boot loads
# (`<app>/cds.classlist`, tools/record-cds-classlists.sh): the JVM maps them instead of loading,
# verifying and linking them, and the relay accepted connections 2.5-2.7 s after its process
# started instead of 4.2 s. Built with the image, so it matches it, the first boot of a new image
# included, and nothing is written at run time. A class the list lacks is loaded the slow way; one
# the code no longer has is skipped. -cp is the start script's own classpath: an archive is used
# only under the classpath it was dumped with.
COPY relay/cds.classlist /app/relay/cds.classlist
COPY sync/cds.classlist /app/sync/cds.classlist
RUN for app in relay sync; do \
      cp=$(sed -n 's/^CLASSPATH=//p' /app/$app/bin/vespa-$app | sed "s#\$APP_HOME#/app/$app#g"); \
      java -Xshare:dump -XX:SharedClassListFile=/app/$app/cds.classlist -XX:SharedArchiveFile=/app/$app/$app.jsa \
        -Xlog:cds=error -cp "$cp" > /tmp/$app-cds.log 2>&1 || { cat /tmp/$app-cds.log; exit 1; }; \
    done && rm -f /tmp/*-cds.log
# Both processes face the network, so neither runs as root. The state directory is theirs; a
# bind mount over it must be writable by this uid (docs/operations.md).
RUN groupadd --system --gid 10001 relay \
    && useradd --system --uid 10001 --gid relay --no-create-home --shell /usr/sbin/nologin relay \
    && mkdir -p /var/lib/vespa-relay \
    && chown relay:relay /var/lib/vespa-relay
USER relay
# Errors only: a mapped archive is silent, and one that does not match is skipped, not fatal.
ENV VESPA_RELAY_OPTS="-XX:SharedArchiveFile=/app/relay/relay.jsa -Xlog:cds=error" \
    VESPA_SYNC_OPTS="-XX:SharedArchiveFile=/app/sync/sync.jsa -Xlog:cds=error"
# The NIP-50 websocket + NIP-11 + web UI (RELAY_PORT, default 7777).
EXPOSE 7777
# The serving relay is the default; /app/sync/bin/vespa-sync is the mirror.
ENTRYPOINT ["/app/relay/bin/vespa-relay"]
