from aiortc import RTCDataChannel, RTCPeerConnection, RTCSessionDescription
from fastapi import APIRouter, Request

from kl_site_common.utils import print_log
from kl_site_server.webrtc import web_rtc_manager
from .model import WebRTCSDPModel

webrtc_router = APIRouter(prefix="/webrtc")


@webrtc_router.post(
    "/offer",
    description="WebRTC offer handling endpoint.",
    response_model=WebRTCSDPModel,
)
async def webrtc_offer(sdp_body: WebRTCSDPModel, request: Request) -> WebRTCSDPModel:
    offer = RTCSessionDescription(sdp=sdp_body.sdp, type=sdp_body.type)

    peer_conn = RTCPeerConnection()
    web_rtc_manager.register_peer_conn(peer_conn)

    print_log(f"Peer connection created for {request.client.host}")

    @peer_conn.on("datachannel")
    def on_datachannel(channel: RTCDataChannel):
        print_log(f"Channel created: {channel.label} ({channel.id})")

        web_rtc_manager.register_data_channel(channel)

        @channel.on("message")
        def on_message(message):
            print_log(f"Received message from channel ({channel.label} / {channel.id}): {message}")

    @peer_conn.on("connectionstatechange")
    async def on_connectionstatechange():
        print_log(f"Connection state changed to {peer_conn.connectionState}")
        if peer_conn.connectionState == "failed":
            await peer_conn.close()
            web_rtc_manager.deregister_peer_conn(peer_conn)

    await peer_conn.setRemoteDescription(offer)

    answer = await peer_conn.createAnswer()
    await peer_conn.setLocalDescription(answer)

    return WebRTCSDPModel(
        sdp=peer_conn.localDescription.sdp,
        type=peer_conn.localDescription.type
    )
