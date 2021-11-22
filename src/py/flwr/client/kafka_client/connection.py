from contextlib import contextmanager
from logging import DEBUG
from queue import Queue
from typing import Callable, Iterator, Tuple

from flwr.common import KAFKA_MAX_MESSAGE_LENGTH

"""Provides contextmanager which manages a Kafka producer & consumer
for the client"""
ServerMessage = "ServerMessage"
ClientMessage = "ClientMessage"

@contextmanager
def kafka_client_connection(
    server_address: str, max_message_length: int = KAFKA_MAX_MESSAGE_LENGTH
) -> Iterator[Tuple[Callable[[], ServerMessage], Callable[[ClientMessage], None]]]:
    """Establish a producer and consumer for client"""
    # start receiver in a new thread
    consumer_channel = MsgReceiver(
        server_address,
        options=[
            ("max_send_message_length", max_message_length),
            ("max_receive_message_length", max_message_length),
        ],
    )
    producer_channel = MsgSender(
        server_address,
        options=[
            ("max_send_message_length", max_message_length),
            ("max_receive_message_length", max_message_length),
        ],
    )
    # Have a Q for messages that need to be sent
    # Confused why there is only a single Q
    queue: Queue[ClientMessage] = Queue(  # pylint: disable=unsubscriptable-object
        maxsize=10 # Should we have a limit?
    )
    # stub = FlowerServiceStub(channel)

    # server_message_iterator: Iterator[ServerMessage] = iter(queue.get, None)

    # This is to receive messages from server
    # Has to be a separate consumer
    # receive: Callable[[], ServerMessage] = lambda: next(server_message_iterator)
    # send: Callable[[ClientMessage], None] = lambda msg: queue.put(msg, block=False)
    receive: Callable = consumer_channel.receive
    send: Callable = producer_channel.send

    try:
        yield (receive, send)
    finally:
        # Make sure to have a final
        consumer_channel.close()
        producer_channel.close()
        # producer_channel.close()
        # log(DEBUG, "Kafca Client Closed")

