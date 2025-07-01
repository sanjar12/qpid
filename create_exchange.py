#!/usr/bin/env python3

import sys
import argparse
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container


BROKER_URL = "localhost:6600" 
MANAGEMENT_NODE_ADDRESS = "qmf.default.direct/broker"

class QmfManager(MessagingHandler):
    def __init__(self, broker_url, method_name, method_arguments):
        super(QmfManager, self).__init__()
        self.broker_url = broker_url
        self.method_name = method_name
        self.method_arguments = method_arguments
        self._sender = None
        self._receiver = None
        self._request_sent = False
        self._connection = None
        self._reply_received = False

    def on_start(self, event):
        self._connection = event.container.connect(self.broker_url)
        self._sender = event.container.create_sender(self._connection, MANAGEMENT_NODE_ADDRESS)
        self._receiver = event.container.create_receiver(self._connection, None, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self._receiver and not self._request_sent:
            self._send_method_request()
            self._request_sent = True

    def _send_method_request(self):
        reply_to_address = self._receiver.remote_source.address

        request_props = {'qmf.opcode': '_method'}
        
        request_body = {
            '_method_name': self.method_name,
            '_arguments': self.method_arguments
        }

        msg = Message(
            reply_to=reply_to_address,
            properties=request_props,
            body=request_body
        )

        print(f"Sending method request '{self.method_name}'...")
        self._sender.send(msg)

    def on_message(self, event):
        self._reply_received = True
        reply_props = event.message.properties
        if reply_props and reply_props.get('qmf.opcode') == '_exception':
            print("\n[ERROR] Broker returned an exception.")
            print(f"Details: {event.message.body}")
        else:
            print("\n[SUCCESS] Broker response received. Operation should be complete.")
        self._connection.close()
    
    def on_disconnected(self, event):
        if not self._reply_received:
            print("\n[ERROR] Connection closed by broker before a reply was received.")
            print("[HINT] This often means a permission error or an invalid property.")
            print("[HINT] Check broker logs for details (journalctl -u qpidd -f).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create an AMQP 1.0 exchange on a Qpid C++ broker.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("exchange_type", help="The type of the exchange (e.g., direct, topic, fanout, headers).")
    parser.add_argument("exchange_name", help="The name of the exchange.")
    parser.add_argument("--durable", action="store_true", help="Make the exchange durable.")
    parser.add_argument("--alternate-exchange", metavar="<name>", help="Name of an alternate exchange for unroutable messages.")
    
    opts = parser.parse_args()

    exchange_properties = {
        "exchange-type": opts.exchange_type
    }

    if opts.durable:
        exchange_properties["durable"] = True
    
    if opts.alternate_exchange:
        exchange_properties["alternate-exchange"] = opts.alternate_exchange

    create_method_arguments = {
        "type": "exchange",
        "name": opts.exchange_name,
        "properties": exchange_properties,
        "strict": True
    }
    
    print("--- Arguments for 'create' method ---")
    print(create_method_arguments)
    print("---------------------------------------")

    try:
        handler = QmfManager(BROKER_URL, "create", create_method_arguments)
        Container(handler).run()
    except Exception as e:
        print(f"An unhandled error occurred: {e}")
