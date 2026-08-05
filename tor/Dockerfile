# The router's Tor transport: a SOCKS5 proxy on the compose network, and
# nothing else. Its own image rather than a layer on the relay image, because
# that one image carries BOTH processes — installing a daemon in it would put
# Tor in the serving relay too, give the container a second thing to supervise,
# and make `restart sync` (a router.conf edit, meant to be cheap) drop every
# circuit and re-bootstrap.
#
# Alpine's package rather than a third-party torproxy image: this is six lines,
# and the images that wrap it are mostly unmaintained forks pinned to old Tor.
FROM alpine:3.22
RUN apk add --no-cache tor
COPY torrc /etc/tor/torrc
# Owned by the user that will run: tor refuses to start on a DataDirectory it
# cannot write, and the torrc names the path explicitly rather than trusting a
# packaging default — a crash loop here reads as "Tor is broken", not as a
# permission bit.
RUN mkdir -p /var/lib/tor && chown -R tor:tor /var/lib/tor && chmod 700 /var/lib/tor
USER tor
EXPOSE 9050
ENTRYPOINT ["/usr/bin/tor", "-f", "/etc/tor/torrc"]
