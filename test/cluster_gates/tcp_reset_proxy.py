#!/usr/bin/env python3
"""A TCP forwarder that can RESET every connection it is carrying, on demand.

Used by fault_injection_gate.sh to inject the ACTUAL fault [D6] is a theory about -- a
burst of TCP connection resets between two live nodes -- instead of waiting for ambient
load to produce one.

WHY A RESET AND NOT A KILLED PEER. The two are different faults and only one of them is
[D6]. Killing a node makes its port refuse connections, so every reconnect fails and the
write SHOULD eventually fail (fail-closed); that case is already covered by the node-kill
gate. [D6] is the other one: the peer is HEALTHY and its listener is open, but an
established connection dies. seastar's rpc::client never re-dials, so the transport must
notice and replace it, and the write retry must outlive the backoff it does that behind.
This proxy reproduces exactly that -- the listening socket stays open the whole time, so
a reconnect succeeds immediately; only the established connections are destroyed.

The reset is a real RST (SO_LINGER with a zero timeout), not a FIN, because a clean
shutdown is the easy case: the peer sees EOF at once. An RST is what a middlebox, a
conntrack eviction or a peer's socket teardown actually produces, and it can surface as a
mid-frame error on either side.

Usage:
    tcp_reset_proxy.py --map LISTEN_HOST:LISTEN_PORT:TARGET_HOST:TARGET_PORT[:reset] ...

`:reset` marks a mapping as resettable; mappings without it are plain forwarders (the
gate proxies a peer's HTTP and Raft ports too, because one peer address serves all three,
but injects only on the data plane).

Signals:
    SIGUSR1  reset every live resettable connection now
Prints one `RESET n` line per round and `READY` once every listener is bound, both
line-buffered so the gate can count them.
"""
import argparse
import asyncio
import signal
import socket
import struct
import sys

BUF = 1 << 16

live = []  # [(writer_a, writer_b)] for resettable mappings only
reset_rounds = 0
reset_conns = 0


def hard_reset(writer):
    """Destroy a connection with an RST rather than a FIN."""
    try:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            # SO_LINGER {on=1, timeout=0} => close() sends RST and discards the queue.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    except OSError:
        pass
    try:
        writer.transport.abort()
    except Exception:
        pass


async def pump(reader, writer):
    try:
        while True:
            data = await reader.read(BUF)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle(client_reader, client_writer, target, resettable):
    try:
        server_reader, server_writer = await asyncio.open_connection(*target)
    except OSError:
        client_writer.close()
        return
    entry = (client_writer, server_writer)
    if resettable:
        live.append(entry)
    try:
        await asyncio.gather(
            pump(client_reader, server_writer),
            pump(server_reader, client_writer),
        )
    finally:
        if resettable and entry in live:
            live.remove(entry)
        for w in entry:
            try:
                w.close()
            except Exception:
                pass


def do_reset():
    global reset_rounds, reset_conns
    victims = list(live)
    live.clear()
    for a, b in victims:
        hard_reset(a)
        hard_reset(b)
    reset_rounds += 1
    reset_conns += len(victims)
    print(f"RESET {len(victims)}", flush=True)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--map", action="append", required=True)
    args = ap.parse_args()

    servers = []
    for m in args.map:
        parts = m.split(":")
        resettable = len(parts) == 5 and parts[4] == "reset"
        lh, lp, th, tp = parts[0], int(parts[1]), parts[2], int(parts[3])
        srv = await asyncio.start_server(
            lambda r, w, t=(th, tp), rs=resettable: handle(r, w, t, rs),
            lh,
            lp,
            reuse_address=True,
            backlog=512,
        )
        servers.append(srv)
        print(f"MAP {lh}:{lp} -> {th}:{tp}{' RESETTABLE' if resettable else ''}", flush=True)

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, do_reset)
    print("READY", flush=True)
    await asyncio.gather(*(s.serve_forever() for s in servers))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
