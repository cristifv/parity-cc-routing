"""A loopback TCP proxy whose link can be cut (phase-6 chaos: an RE leader cut off from etcd but not
from its clients). While <cut-file> exists, nothing is forwarded in either direction and new
connections stall too (a partition, not a refusal: no RST, no FIN). Usage:
  python3 -I tcp_proxy.py <listen-port> <target-host:port> <cut-file> <port-file>"""
import asyncio
import os
import sys

LISTEN, TARGET, CUT, PORTFILE = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
HOST, TPORT = TARGET.rsplit(":", 1)


async def pump(reader, writer):
    try:
        while True:
            while os.path.exists(CUT):
                await asyncio.sleep(0.05)
            data = await reader.read(65536)
            if not data:
                break
            while os.path.exists(CUT):
                await asyncio.sleep(0.05)
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle(cr, cw):
    while os.path.exists(CUT):
        await asyncio.sleep(0.05)
    try:
        tr, tw = await asyncio.open_connection(HOST, int(TPORT))
    except OSError:
        cw.close()
        return
    await asyncio.gather(pump(cr, tw), pump(tr, cw))


async def main():
    srv = await asyncio.start_server(handle, "127.0.0.1", LISTEN)
    port = srv.sockets[0].getsockname()[1]
    with open(PORTFILE, "w") as f:
        f.write(str(port))
    async with srv:
        await srv.serve_forever()


asyncio.run(main())
