#!/usr/bin/env python
import pika

def main():
    # Set up the connection to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    # Declare the queue (must match the producer's settings)
    channel.queue_declare(
        queue='service-queue', 
        durable=False, 
        exclusive=False, 
        auto_delete=False, 
        arguments=None
    )
    
    # Counter for received messages
    message_count = 0

    # Callback function to handle incoming messages
    def callback(ch, method, properties, body):
        nonlocal message_count
        message_count += 1
        message = body.decode('utf-8')
        print(f"Received message #{message_count}: {message}")

    # Subscribe to the queue
    channel.basic_consume(
        queue='service-queue',
        on_message_callback=callback,
        auto_ack=True
    )

    print("Subscribed to the queue 'service-queue'. Waiting for messages...")
    try:
        # Start consuming messages
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupted. Exiting...")
        connection.close()

if __name__ == '__main__':
    main()
