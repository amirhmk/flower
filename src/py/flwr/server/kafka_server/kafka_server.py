# Copyright 2020 Adap GmbH. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Implements utility function to create a grpc server."""
import queue
import time
import json
from threading import Thread
from typing import Dict
from flwr.common import KAFKA_MAX_SIZE
from flwr.common.logger import log
from flwr.proto.transport_pb2 import ClientMessage, ServerMessage
from flwr.proto import transport_pb2 as flwr_dot_proto_dot_transport__pb2
from logging import INFO, DEBUG
from flwr.server.client_manager import ClientManager
from flwr.server.kafka_server import flower_service_servicer as fss
from kafka_consumer.consumer import MsgReceiver
from kafka_producer.producer import MsgSender
# from flwr.server.app import SERVER_TOPIC


def start_kafka_receiver(
    client_manager: ClientManager,
    server_address: str,
    max_concurrent_workers: int = 1000,
    max_message_length: int = KAFKA_MAX_SIZE,
    topic_name = None
) :
    """Create kafka server and return registered FlowerServiceServicer instance.

    If used in a main function server.wait_for_termination(timeout=None)
    should be called as otherwise the server will immediately stop.

    Parameters
    ----------
    client_manager : ClientManager
        Instance of ClientManager
    server_address : str
        Server address in the form of HOST:PORT e.g. "[::]:8080"
    max_concurrent_workers : int
        Set the maximum number of clients you want the server to process
        before returning RESOURCE_EXHAUSTED status (default: 1000)
    max_message_length : int
        Maximum message length that the server can send or receive.
        Int valued in bytes. -1 means unlimited. (default: GRPC_MAX_MESSAGE_LENGTH)

    Returns
    -------
    server : kafka receiver
        An instance of a receiver which is already started
    """

    server = KafkaServer(server_address, max_message_length, client_manager, topic_name)
    server.startServer()
    return server

class KafkaServer:
    def __init__(self, server_address : str, max_message_length : int, 
                       client_manager : ClientManager, topic_name : str) -> None:
        self.server_address = server_address
        self.client_manager = client_manager
        self.topic_name = topic_name
        self.max_message_length = max_message_length
        self.registered_cids = dict()        
        self.clientmsg_deserializer=flwr_dot_proto_dot_transport__pb2.ClientMessage.FromString,
        self.serverresponse_serializer=flwr_dot_proto_dot_transport__pb2.ServerMessage.SerializeToString,


        pass
    def stopServer(self, grace=1):
        time.sleep(grace)
        self.running = False
        self.thread.interrupt()
    def startServer(self):
        self.running = True
        self.__startServerReceiver()
        self.__initServerMsgSender()
    
    def __startServerReceiver(self):
        self.serverReceiver = MsgReceiver(self.server_address,
                             options={
                                "max_send_message_length": self.max_message_length,
                                "max_receive_message_length": self.max_message_length,
                                "topic_name": self.topic_name
                             },
        )
        self.servicer = fss.FlowerServiceServicer(self.client_manager)
        self.serverReceiver.start()
        self.thread = Thread(target = self.receiveMsgs, args = ())
        self.thread.start()
    
    def __initServerMsgSender(self):
        self.server_msg_sender = MsgSender(
            self.server_address,
            options={
                "max_send_message_length": self.max_message_length,
                "max_receive_message_length": self.max_message_length,
            },
        )

    def receiveMsgs(self):
        log(INFO, "Starting server receiver thread")
        while(self.running):
            msg = self.serverReceiver.getNextMsg(block=True, timeout=1000)
            if not self.running:
                log(INFO,"Kafka server interrupted")
                break
            if msg is None:
                log(DEBUG,"No message received")
                continue
            log(INFO,"Got new message!")

            #need to deserialize msg, get cid and push the msg to bridge
            cid, clientmsg = self.getClientMessage(msg)
            print(clientmsg)
            if self.registered_cids.has(cid):
                q = self.registered_cids.get(cid)
            else:
                q = queue()
                inputiterator = self.__inputmsg(q)
                self.registered_cids.set(cid, q)
                self.thread = Thread(target = self.servermsgSender, args = (cid,inputiterator))
                self.thread.start()
            if clientmsg is not None:
                log(INFO, f"Pushing new msg to cid {cid}")
                q.add(clientmsg)
                log(DEBUG, f"Done pushing msg to cid {cid}")
            else:
                log(INFO, f"Received registration for cid {cid}")
        log(INFO, "Stopping server receiver thread")
    
    def __inputmsg(self, q):
        yield q.get()

    def servermsgSender(self, cid, inputiterator):
        log(INFO, f"Starting server sender thread for {cid}")
        servermsgIterator = self.servicer.Join(inputiterator, cid)
        #returns iterator with next msg from server to client
        while self.running:
            try:
                msg : ServerMessage = next(servermsgIterator)
                msgdata = self.getServerMessageBinary(cid, msg)
                self.server_msg_sender.sendMsg(msgdata, f"FLclient{cid}")
            except:
                if not self.running:
                    break
        log(INFO, f"Stopped server sender thread for {cid}")


    def getServerMessageBinary(self, cid : str, servermsg : ServerMessage):
        payloadstr = servermsg.SerializeToString()
        payload = {"cid" : cid, "payload" : str(payloadstr.hex())}
        return str(payload).encode('utf-8')
    def getClientMessage(self, msgdata) -> tuple([str, ClientMessage]):
        jdata = json.load(msgdata)
        cid = jdata['cid']
        if len(jdata['payload']) == 0:
            clientmsg = None
        else:
            clientmsg = ClientMessage.FromString(bytes.fromhex(jdata['payload']))
        return cid, clientmsg
    