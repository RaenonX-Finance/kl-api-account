import zmq

from tcoreapi_mq.model import ZmqSocket


def create_socket(socket_type: int, timeout_ms: int | None = None) -> ZmqSocket:
    socket = zmq.Context().socket(socket_type)

    if timeout_ms:
        socket.RCVTIMEO = timeout_ms

    return ZmqSocket(socket)


def create_subscription_receiver_socket(subscription_port: int, timeout_ms: int | None = None) -> ZmqSocket:
    socket = create_socket(zmq.SUB, timeout_ms)
    socket.connect_local(subscription_port)
    socket.set_subscribe()

    return socket
