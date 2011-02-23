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

class Listener():
    """ Simple class to wrap the operations needed for an AMQP listener """

    def __init__(self, hostname="127.0.0.1", userid="guest", password="guest",
                 virtual_host="/", port=5672):
        """ Setup a connection to the AMQP server, get a channel 
            Create a topic exchange, attach a bonded queue to it
            and register a consumer callback.
            
            A specific service listener implementation overrides the name 
            and routing_key
        """

        self.connection = BrokerConnection(hostname=hostname, 
                                           userid=userid, password=password, 
                                           virtual_host=virtual_host, port=443,
                                           insist=False, ssl=False)
        self.channel = self.connection.channel()
        self.exchange = Exchange(name=self.name, type="topic", durable=True,
                                 channel=self.channel)
        self.queue = Queue(self.name, exchange=self.exchange,
                           routing_key=self.routing_key)
        self.queue = self.queue(self.channel)
        self.queue.declare()
        self.queue.consume(consumer_tag="", callback=self.callback, no_ack=True)
        self.connection.connect()
        return

    def callback(self, msg):
        """ This callback is run when a message is recieved """
        return

    def consume(self):
        """ Event loop """
        while True:
            self.connection.drain_events()
        return

