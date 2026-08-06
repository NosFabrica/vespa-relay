# Tor, twice over: the SOCKS proxy the router dials .onion upstreams THROUGH,
# and the hidden service that puts the relay itself ON Tor. One image, two
# configs, two containers — the jobs share nothing but the binary, and running
# them in one daemon would tie the relay's front door to the router's restarts
# (`restart sync` is a router.conf edit, meant to be cheap; re-publishing a
# hidden-service descriptor is not).
#
# Its own image rather than a layer on the relay image, because that one image
# carries BOTH JVM processes — installing a daemon in it would put Tor in the
# serving relay too and give the container a second thing to supervise.
#
# Alpine's package rather than a third-party torproxy image: this is a handful
# of lines, and the images that wrap it are mostly unmaintained forks pinned to
# old Tor.
FROM alpine:3.22
RUN apk add --no-cache tor
# The client proxy (default), and the hidden service the `tor-onion` service
# selects with its own entrypoint.
COPY torrc /etc/tor/torrc
COPY torrc-onion /etc/tor/torrc-onion
COPY publish-onion.sh /usr/local/bin/publish-onion
# chmod in its own step rather than trusting the checked-in file's mode: a lost
# executable bit surfaces as a container that will not start, and `COPY
# --chmod` needs BuildKit.
RUN chmod 755 /usr/local/bin/publish-onion
# Owned by the user that will run: tor refuses to start on a DataDirectory it
# cannot write, and the torrc names the path explicitly rather than trusting a
# packaging default — a crash loop here reads as "Tor is broken", not as a
# permission bit.
RUN mkdir -p /var/lib/tor && chown -R tor:tor /var/lib/tor && chmod 700 /var/lib/tor
USER tor
EXPOSE 9050
ENTRYPOINT ["/usr/bin/tor", "-f", "/etc/tor/torrc"]
