#!/usr/bin/env python3

import sys
import pika

# --- Configuration ---
# NOTE: We connect to the default AMQP 0-10 port!
BROKER_HOST = "localhost"
BROKER_PORT = 5672

def bind_queue_to_exchange(exchange_name, queue_name, routing_key):
    """Connects to the broker and binds a queue to an exchange."""
    connection = None
    try:
        params = pika.ConnectionParameters(host=BROKER_HOST, port=BROKER_PORT)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Bind the queue. This is the core operation.
        # Note: This will fail if the queue or exchange do not exist.
        print(f"Binding queue '{queue_name}' to exchange '{exchange_name}' with key '{routing_key}'...")
        channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=routing_key
        )
        print("[SUCCESS] Queue bound successfully.")

    except pika.exceptions.ChannelError as e:
        print(f"[ERROR] Could not bind. Does the queue or exchange exist?")
        print(f"Broker responded: {e}")
        sys.exit(1)
    except pika.exceptions.AMQPConnectionError as e:
        print(f"[ERROR] Could not connect to {BROKER_HOST}:{BROKER_PORT}. Is qpidd running?")
        print(f"Details: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        if connection and connection.is_open:
            connection.close()

# --- Main execution ---
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 bind_queue.py <exchange-name> <queue-name> <binding-key>")
        print("Example: python3 bind_queue.py s1 s2 mykey")
        sys.exit(1)

    bind_queue_to_exchange(
        exchange_name=sys.argv[1],
        queue_name=sys.argv[2],
        routing_key=sys.argv[3]
    )
