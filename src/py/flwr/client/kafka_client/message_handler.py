
import ast
import json
import numpy as np

from typing import Tuple
from logging import DEBUG

from flwr.client.client import Client
from flwr.common import KafkaMessage
from kafka_producer.producer import MsgSender
from flwr.common.logger import log
from flwr.common import parser
from flwr.common.parameter import parameters_to_weights


def KafkaClientMessage(type: str, payload: dict) -> KafkaMessage:
    return {
        "type": type,
        "payload": payload
    }


class UnknownServerMessage(Exception):
    """Signifies that the received message is unknown."""

def handle_kafka(
    client: Client, server_msg
) -> Tuple[KafkaMessage, int, bool]:
    print("what do we have here", server_msg.value)
    decoded_response = json.loads(server_msg.value)
    # decoded_response = ast.literal_eval(server_msg.value.decode("utf8"))
    # print("des", des, type(des))
    if decoded_response['type'] == "fit_ins":
        return _fit(client, decoded_response), 0, True
    if decoded_response['type'] == "reconnect":
        disconnect_msg, sleep_duration = _reconnect(server_msg.reconnect)
        return disconnect_msg, sleep_duration, False
    if decoded_response['type'] == "get_parameters":
        return _get_parameters(client), 0, True
    if decoded_response['type'] == "fit_ins":
        return _fit(client, server_msg.fit_ins), 0, True
    if decoded_response['type'] == "evaluate_ins":
        return _evaluate(client, server_msg.evaluate_ins), 0, True
    if decoded_response['type'] == "properties_ins":
        return _get_properties(client, server_msg.properties_ins), 0, True
    raise UnknownServerMessage()


def _get_parameters(client: Client) -> KafkaClientMessage:
    # No need to deserialize get_parameters_msg (it's empty)
    parameters_res = client.get_parameters()
    # parameters_res_proto = serde.parameters_res_to_proto(parameters_res)
    # return parameters_res
    payload = { "parameters": parameters_res.parameters.tensors }
    return KafkaClientMessage(type="parameters_res", payload={})


def _get_properties(
    client: Client, properties_msg
) -> KafkaMessage:
    # Deserialize get_properties instruction
    properties_ins = parse_get_properties_ins(properties_msg)
    # Request for properties
    properties_res = client.get_properties(properties_msg)
    # Serialize response
    payload = { "properties": properties_res }
    return KafkaClientMessage(type="properties_res", payload={})


def _fit(client: Client, fit_msg) -> KafkaMessage:
    # Deserialize fit instruction
    server_address, fit_ins = parser.fit_ins_from_kafka(fit_msg)
    sendMsg = start_train(server_address, fit_ins)
    # Perform fit
    fit_res = client.fit(fit_ins)
    # Serialize fit result
    fit_res_kafka = parser.fit_res_to_kafka(fit_res)
    return sendMsg, KafkaClientMessage(type="fit_res", payload=fit_res_kafka)


def _evaluate(client: Client, evaluate_msg) -> KafkaMessage:
    # Deserialize evaluate instruction
    # evaluate_ins = serde.evaluate_ins_from_proto(evaluate_msg)
    evaluate_ins = parser.parse_evaluate_ins(evaluate_msg)
    # Perform evaluation
    evaluate_res = client.evaluate(evaluate_ins)
    # Serialize evaluate result
    payload = { "properties": evaluate_res }
    return KafkaClientMessage(type="evaluate_res", payload={})


def _reconnect(
    reconnect_msg,
) -> Tuple[KafkaMessage, int]:
    # Determine the reason for sending Disconnect message
    reason = Reason.ACK
    sleep_duration = None
    if reconnect_msg.seconds is not None:
        reason = Reason.RECONNECT
        sleep_duration = reconnect_msg.seconds
    # Build Disconnect message
    disconnect = ClientMessage.Disconnect(reason=reason)
    return ClientMessage(disconnect=disconnect), sleep_duration

def start_train(server_address, train_ins):
    # start producer in a new thread
    producer_channel = MsgSender(
        server_address,
        options=train_ins.config,
    )
    log(DEBUG, f"Started Kafka Producer to topic={train_ins.config['topic_name']}")
    return producer_channel.sendMsg