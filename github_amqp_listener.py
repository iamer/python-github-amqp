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


class githubPayload(object):
    """ Class to wrap the payload json received from github """

    def __init__(self, msg):
        self.payload = json.loads( msg.body )['payload']

    def __getattr__(self, attr):
        return self.payload.get(attr,None)

    def __str__(self):
        return json.dumps(self.payload, sort_keys=True, indent=4)

class githubListener():
    """ Simple class to wrap the operations needed for an AMQP listener """

    def __init__(self, hostname="127.0.0.1", userid="guest", password="guest",
                 virtual_host="/", port=5672):
        """ Setup a connection to the AMQP server, get a channel 
            Create a topic exchange, attach a bonded queue to it
            Register a consumer callback """

        self.connection = BrokerConnection(hostname=hostname, 
                                           userid=userid, password=password, 
                                           virtual_host=virtual_host, port=443,
                                           insist=False, ssl=False)
        self.channel = self.connection.channel()
        self.exchange = Exchange(name="githubpush", type="topic", durable=True,
                                 channel=self.channel)
        self.queue = Queue("githubpush", exchange=self.exchange,
                           routing_key="github.push.#")
        self.queue = self.queue(self.channel)
        self.queue.declare()
        self.queue.consume(consumer_tag="", callback=self.callback, no_ack=True)
        self.connection.connect()
        return

    def callback(self, msg):
        """ This callback is run when a message is recieved """
        print "Got a message ..."
        x = githubPayload(msg)
        print x
        return

    def consume(self):
        """ Event loop """
        print "Waiting for a message ..."
        while True:
            self.connection.drain_events()
        return


if __name__ == "__main__":
    try:
        c = githubListener(hostname="127.0.0.1", 
                           userid="guest", password="guest",
                           virtual_host="/", port=5672)
        print "Connected to AMQP server ..."
        c.consume()
    except KeyboardInterrupt:
        c.connection.close()
        print "Bye!"
        sys.exit(0)
    except Exception, e:
        print Exception
        print e

