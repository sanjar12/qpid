#!/usr/bin/env python3

import sys
import argparse
import json
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

# AMQP 1.0 management address
BROKER_URL = "localhost:5672"
MANAGEMENT_ADDRESS = "$management"

class AmqpManager(MessagingHandler):
    def __init__(self, broker_url, operation, entity_type, entity_name, properties=None):
        super(AmqpManager, self).__init__()
        self.broker_url = broker_url
        self.operation = operation
        self.entity_type = entity_type
        self.entity_name = entity_name
        self.properties = properties or {}
        self._sender = None
        self._receiver = None
        self._connection = None
        self._correlation_id = "create-exchange-001"

    def on_start(self, event):
        self._connection = event.container.connect(self.broker_url)
        self._sender = event.container.create_sender(self._connection, MANAGEMENT_ADDRESS)
        self._receiver = event.container.create_receiver(self._connection, None, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self._receiver:
            self._send_management_request()

    def _send_management_request(self):
        reply_to = self._receiver.remote_source.address
        
        # AMQP 1.0 management message format
        request_body = {
            "operation": self.operation,
            "type": self.entity_type,
            "name": self.entity_name
        }
        
        # Add properties if provided
        if self.properties:
            request_body.update(self.properties)

        msg = Message(
            body=request_body,
            reply_to=reply_to,
            correlation_id=self._correlation_id,
            properties={
                "operation": self.operation,
                "type": self.entity_type,
                "name": self.entity_name
            }
        )

        print(f"Sending {self.operation} request for {self.entity_type} '{self.entity_name}'...")
        print(f"Request body: {json.dumps(request_body, indent=2)}")
        self._sender.send(msg)

    def on_message(self, event):
        reply = event.message
        
        print(f"\nReceived reply with correlation_id: {reply.correlation_id}")
        
        if hasattr(reply, 'properties') and reply.properties:
            status_code = reply.properties.get('statusCode', 200)
            status_description = reply.properties.get('statusDescription', 'OK')
            
            if status_code >= 200 and status_code < 300:
                print(f"[SUCCESS] {self.entity_type.capitalize()} '{self.entity_name}' {self.operation}d successfully")
                print(f"Status: {status_code} - {status_description}")
            else:
                print(f"[ERROR] Operation failed with status {status_code}: {status_description}")
        else:
            print(f"[SUCCESS] {self.entity_type.capitalize()} '{self.entity_name}' {self.operation}d successfully")
        
        if reply.body:
            print(f"Response body: {reply.body}")
            
        self._connection.close()

    def on_disconnected(self, event):
        print(f"Disconnected from broker")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create an AMQP 1.0 exchange on a Qpid C++ broker using management interface.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("exchange_type", help="The type of the exchange (e.g., direct, topic, fanout, headers).")
    parser.add_argument("exchange_name", help="The name of the exchange.")
    parser.add_argument("--durable", action="store_true", help="Make the exchange durable.")
    parser.add_argument("--auto-delete", action="store_true", help="Auto-delete the exchange when no longer in use.")
    parser.add_argument("--alternate-exchange", metavar="<name>", help="Name of an alternate exchange.")
    parser.add_argument("--argument", dest="extra_arguments", action="append", default=[], 
                        metavar="<NAME=VALUE>", help="Additional exchange arguments.")

    opts = parser.parse_args()

    # Build properties
    properties = {
        "exchangeType": opts.exchange_type,
        "durable": opts.durable,
        "autoDelete": opts.auto_delete
    }

    if opts.alternate_exchange:
        properties["alternateExchange"] = opts.alternate_exchange

    # Handle extra arguments
    arguments = {}
    for arg in opts.extra_arguments:
        if "=" not in arg:
            print(f"[ERROR] Invalid format for --argument: '{arg}'. Must be NAME=VALUE.")
            sys.exit(1)
        key, value = arg.split("=", 1)
        try:
            arguments[key] = int(value)
        except ValueError:
            if value.lower() == 'true':
                arguments[key] = True
            elif value.lower() == 'false':
                arguments[key] = False
            else:
                arguments[key] = value

    if arguments:
        properties["arguments"] = arguments

    print("--- Exchange creation request ---")
    print(f"Name: {opts.exchange_name}")
    print(f"Type: {opts.exchange_type}")
    print(f"Properties: {json.dumps(properties, indent=2)}")
    print("--------------------------------")

    try:
        handler = AmqpManager(BROKER_URL, "CREATE", "org.apache.qpid.broker:exchange", 
                             opts.exchange_name, properties)
        Container(handler).run()
    except Exception as e:
        print(f"An error occurred: {e}")
