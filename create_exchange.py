#!/usr/bin/env python3

import sys
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

BROKER_URL = "localhost:6600"
MANAGEMENT_NODE_ADDRESS = "qmf.default.direct/broker"

class QmfManager(MessagingHandler):
    def __init__(self, broker_url, exchange_properties):
        super(QmfManager, self).__init__()
        self.broker_url = broker_url
        self.exchange_properties = exchange_properties
        self._sender = None
        self._receiver = None
        self._request_sent = False
        self._connection = None
        self._reply_received = False

    def on_start(self, event):
        print("Handler started. Connecting...")
        self._connection = event.container.connect(self.broker_url)
        self._sender = event.container.create_sender(self._connection, MANAGEMENT_NODE_ADDRESS)
        self._receiver = event.container.create_receiver(self._connection, None, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self._receiver and not self._request_sent:
            print("Reply-to link opened. Sending management request.")
            self._send_create_request()
            self._request_sent = True

    def _send_create_request(self):
        reply_to_address = self._receiver.remote_source.address
        request_props = {
            'qmf.opcode': '_create',
            'qmf.schema_id': {'package': 'org.apache.qpid.broker', 'class': 'exchange'}
        }
        msg = Message(
            reply_to=reply_to_address,
            properties=request_props,
            body=self.exchange_properties
        )
        print(f"Sending request to create exchange '{self.exchange_properties['name']}'...")
        self._sender.send(msg)

    def on_message(self, event):
        self._reply_received = True
        reply_props = event.message.properties
        print("\n--- Reply Received ---")
        if reply_props and reply_props.get('qmf.opcode') == '_exception':
            print("[ERROR] Broker returned an exception. Exchange creation failed.")
            print(f"Details: {event.message.body}")
        else:
            print("[SUCCESS] Broker response received. Exchange should be created.")
        self._connection.close()

    def on_transport_error(self, event):
        print(f"[DEBUG] Transport error detected: {event.transport.condition}")
        if self._connection:
            self._connection.close()
            
    def on_disconnected(self, event):
        print("[DEBUG] Connection disconnected.")
        if not self._reply_received:
            print("[HINT] Disconnected BEFORE receiving a reply. Check broker logs for authentication/authorization issues.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 create_exchange.py <exchange-name> <exchange-type>")
        sys.exit(1)
    exchange_name = sys.argv[1]
    exchange_type = sys.argv[2]
    properties_to_create = {"name": exchange_name, "type": exchange_type, "durable": True}
    try:
        handler = QmfManager(BROKER_URL, properties_to_create)
        Container(handler).run()
    except Exception as e:
        print(f"An error occurred: {e}")
