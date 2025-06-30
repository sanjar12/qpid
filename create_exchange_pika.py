#!/usr/bin/env python3

import sys
import pika

# --- Configuration ---
# NOTE: We connect to the default AMQP 0-10 port!
BROKER_HOST = "localhost"
BROKER_PORT = 5672

def create_exchange(exchange_name, exchange_type):
    """Connects to the broker and declares a durable exchange."""
    connection = None
    try:
        # Establish a connection to the broker
        params = pika.ConnectionParameters(host=BROKER_HOST, port=BROKER_PORT)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Declare the exchange. This is the core operation.
        # If the exchange already exists with the same properties, this does nothing.
        print(f"Declaring durable '{exchange_type}' exchange named '{exchange_name}'...")
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=True  # Make it survive broker restarts
        )
        print("[SUCCESS] Exchange declared successfully.")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"[ERROR] Could not connect to {BROKER_HOST}:{BROKER_PORT}. Is qpidd running?")
        print(f"Details: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        # Make sure the connection is closed
        if connection and connection.is_open:
            connection.close()

# --- Main execution ---
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 create_exchange.py <exchange-name> <exchange-type>")
        print("Example: python3 create_exchange.py my-direct-exchange direct")
        sys.exit(1)

    create_exchange(exchange_name=sys.argv[1], exchange_type=sys.argv[2])
