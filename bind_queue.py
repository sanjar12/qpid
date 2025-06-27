#!/usr/bin/env python3

import sys
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

# --- Configuration ---
BROKER_URL = "localhost:5672"
MANAGEMENT_NODE_ADDRESS = "qmf.default.direct/broker"

class QmfManager(MessagingHandler):
    def __init__(self, broker_url, bind_arguments):
        super(QmfManager, self).__init__()
        self.broker_url = broker_url
        self.bind_arguments = bind_arguments
        self._sender = None
        self._receiver = None
        self._request_sent = False
        self._connection = None

    def on_start(self, event):
        self._connection = event.container.connect(self.broker_url)
        self._sender = event.container.create_sender(self._connection, MANAGEMENT_NODE_ADDRESS)
        self._receiver = event.container.create_receiver(self._connection, None, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self._receiver and not self._request_sent:
            self._send_bind_request()
            self._request_sent = True

    def _send_bind_request(self):
        reply_to_address = self._receiver.remote_source.address

        # For a 'bind' command, we call a method on the broker object
        request_props = {
            'qmf.opcode': '_method',
        }
        
        request_body = {
            '_method_name': 'bind',
            '_arguments': self.bind_arguments
        }

        msg = Message(
            reply_to=reply_to_address,
            application_properties=request_props,
            body=request_body
        )

        print(f"Sending request to bind queue '{self.bind_arguments['queue']}' to exchange '{self.bind_arguments['exchange']}' with key '{self.bind_arguments['key']}'...")
        self._sender.send(msg)

    def on_message(self, event):
        reply_props = event.message.application_properties
        if reply_props.get('qmf.opcode') == '_exception':
            print("\n[ERROR] Broker returned an exception. Bind operation failed.")
            print(f"Details: {event.message.body}")
            print("\nHint: Make sure both the exchange and the queue already exist.")
        else:
            print(f"\n[SUCCESS] Broker response received. Bind operation should be complete.")
        self._connection.close()

    def on_transport_error(self, event):
        print(f"[ERROR] Transport error: {event.transport.condition}")
        if self._connection:
            self._connection.close()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 bind_queue.py <exchange-name> <queue-name> <binding-key>")
        print("Example: python3 bind_queue.py my-app-exchange my-app-queue my.routing.key")
        sys.exit(1)

    exchange_name = sys.argv[1]
    queue_name = sys.argv[2]
    binding_key = sys.argv[3]

    arguments_for_bind = {
        "exchange": exchange_name,
        "queue": queue_name,
        "key": binding_key
    }

    try:
        handler = QmfManager(BROKER_URL, arguments_for_bind)
        Container(handler).run()
    except Exception as e:
        print(f"An error occurred: {e}")
