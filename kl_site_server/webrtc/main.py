import asyncio
from collections import defaultdict
from typing import Any

from aiortc import RTCDataChannel, RTCPeerConnection

from kl_site_common.utils import execute_async_function, print_log


class WebRTCManager:
    def __init__(self):
        self._peer_conns: set[RTCPeerConnection] = set()
        self._data_chs: defaultdict[str, set[RTCDataChannel]] = defaultdict(set)

    def register_peer_conn(self, peer_conn: RTCPeerConnection):
        self._peer_conns.add(peer_conn)

    def deregister_peer_conn(self, peer_conn: RTCPeerConnection):
        self._peer_conns.discard(peer_conn)

    def register_data_channel(self, channel: RTCDataChannel):
        self._data_chs[channel.label].add(channel)

    async def send_data_to_channels(self, label: str, data: Any):
        # https://github.com/zeromq/pyzmq/issues/1423
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        channels = self._data_chs[label]
        print_log(f"Sending data to {len(channels)} channels")

        closed_ch = set()

        for channel in channels:
            if channel.readyState != "open":
                print_log(f"Channel ({channel.label} / {channel.id}) closed (state: {channel.readyState})")
                channel.close()
                closed_ch.add(channel)
                continue

            channel.send(data)

            try:
                # https://github.com/aiortc/aiortc/issues/547
                # noinspection PyProtectedMember
                await channel.transport._data_channel_flush()
                # noinspection PyProtectedMember
                await channel.transport._transmit()
            except ConnectionError as ex:
                print_log(
                    f"Channel ({channel.label} / {channel.id}) connection error - "
                    f"closing ({ex} / state: {channel.readyState})"
                )
                channel.close()
                closed_ch.add(channel)

        self._data_chs[label] -= closed_ch

    def shutdown(self):
        execute_async_function(
            asyncio.gather,
            *(peer_conn.close() for peer_conn in self._peer_conns)
        )

        self._peer_conns.clear()
