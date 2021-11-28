import os
import json
import time
import random
import sys
from contextlib import contextmanager
from logging import DEBUG
from queue import Queue
from typing import Callable, Iterator, Tuple

from kafka_consumer.consumer import MsgReceiver
from kafka_producer.producer import MsgSender

from flwr.proto.transport_pb2 import ClientMessage, ServerMessage
from flwr.common import KAFKA_MAX_MESSAGE_LENGTH
from flwr.common.logger import log
from os.path import expanduser

home = expanduser("~")
SERVER_TOPIC = "FLserver"

"""Provides contextmanager which manages a Kafka producer & consumer
for the client"""
# ServerMessage = "ServerMessage"
# ClientMessage = "ClientMessage"

def getCid():
    cidpath = os.path.join(home, '.flwr')
    cidfile = os.path.join(cidpath, 'cid.txt')
    if os.path.exists(cidpath):
        if os.path.exists(cidfile):
            with open(cidfile, 'r') as f:
                jdata = json.load(f)
                return jdata['cid']
    else:
        os.makedirs(cidpath, exist_ok=True)
    ms = time.time_ns() 
    cid = str(ms) + str(random.randint(0,100))
    dat = {'cid':cid}
    with open(cidfile, 'w') as f:
        json.dump(dat,f)
    return cid

@contextmanager
def kafka_client_connection(
    server_address: str, cid : str, max_message_length: int = KAFKA_MAX_MESSAGE_LENGTH,
) -> Iterator[Tuple[Callable[[], ServerMessage], Callable[[bytes], None]]]:
    """Establish a producer and consumer for client"""
    # start receiver in a new thread

    consumer_topic_name = f"FLclient{cid}"
    consumer_channel = MsgReceiver(
        server_address,
        options={
            "max_send_message_length": max_message_length,
            "max_receive_message_length": max_message_length,
            "topic_name": consumer_topic_name,
            "log" : log
        },
    )
    log(DEBUG, f"Started Kafka Consumer from topic={consumer_topic_name}")
    
    # # start producer in a new thread
    producer_channel = MsgSender(
        server_address,
        options={
            "max_send_message_length": max_message_length,
            "max_receive_message_length": max_message_length,
            "topic_name": SERVER_TOPIC,
            "log" : log
        },
    )
    log(DEBUG, f"Started Kafka Producer to topic={SERVER_TOPIC}")


    # msg = np.random.randint(0, 100, (3,3)) 
    # producer_channel.sendMsg(msg)
    # Have a Q for messages that need to be sent
    # Confused why there is only a single Q
    queue: Queue[ClientMessage] = Queue(  # pylint: disable=unsubscriptable-object
        maxsize=10 # Should we have a limit?
    )

    consumer_channel.start()
    # stub = FlowerServiceStub(channel)

    # server_message_iterator: Iterator[ServerMessage] = iter(queue.get, None)

    # This is to receive messages from server
    # Has to be a separate consumer
    # receive: Callable[[], ServerMessage] = lambda: next(server_message_iterator)

    #send and receive binary data
    send: Callable = lambda msg: producer_channel.sendMsg
    receive: Callable = consumer_channel.getNextMsg
    
    try:
        yield (receive, send)
    except:
        print("Oops!", sys.exc_info()[0], "occurred.")
    finally:
        # Make sure to have a final
        consumer_channel.close()
        # producer_channel.close()
        # producer_channel.close()
        # log(DEBUG, "Kafca Client Closed")

