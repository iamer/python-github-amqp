#!/usr/bin/env python -tt

from vcsamqp import Listener, Payload

class githubListener(Listener):
    """ Implementation of amqp Listener that works with github """

    def __init__(self, hostname="127.0.0.1", userid="guest", password="guest",
                             virtual_host="/", port=5672):
        """ 
        Github uses the routing keys :
            "github.push.#{owner}.#{repo}.#{ref}"
            "github.commit.#{owner}.#{repo}.#{ref}.#{author}"
        """
        self.name = "githubpush"
        self.routing_key="github.push.#"
        Listener.__init__(self, hostname=hostname, userid=userid, 
                          password=password, virtual_host=virtual_host, port=port)
        return

    def callback(self, msg):
        """ This callback is run when a message is recieved """
        print "Got a message ..."
        x = Payload(msg)
        print x
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
#    except Exception, e:
#        print Exception
#        print e

