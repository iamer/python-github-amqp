#!/usr/bin/env python -tt

import json
import sys
# Using kombu from http://ask.github.com/kombu/
# It replaces carrot which is discontinued
# It is a high level abstraction of various messaging transports
# http://ask.github.com/kombu/introduction.html#features
# Currently using the pyamqplib transport but plans to move
# to pika in the future when the latest version is supported
# https://github.com/ask/kombu/issues/#issue/20
from kombu.connection import BrokerConnection
from kombu.messaging import Exchange, Queue, Consumer, Producer

class Sender():
    """ Simple class to wrap the operations needed for an AMQP listener """

    def __init__(self, hostname="127.0.0.1", userid="guest", password="guest",
                 virtual_host="/", port=5672, name="", routing_key=""):
        """ Setup a connection to the AMQP server, get a channel 
            and create an exchange.
            
            A specific service listener implementation overrides the name 
            and routing_key
        """
        if name == "":
            raise Exception("Name must be non-empty string")
        self.name = name
        self.routing_key = routing_key

        if routing_key == "":
            exchange_type = "fanout"
        elif "*" in routing_key or "#" in routing_key:
            exchange_type = "topic"
        else :
            exchange_type = "direct"

        self.connection = BrokerConnection(hostname=hostname, 
                                           userid=userid, password=password, 
                                           virtual_host=virtual_host, port=443,
                                           insist=False, ssl=False)
        self.channel = self.connection.channel()
        self.exchange = Exchange(name=self.name, type=exchange_type, durable=False,
                                 channel=self.channel)
        self.connection.connect()
        return

    def send(self, msg):
        """ Publishes a message to the AMQP server
            on the initialized exchange
            msg is a string, usually a JSON dump
        """
        self.exchange.publish(self.exchange.Message(msg), routing_key=self.routing_key)
        return

