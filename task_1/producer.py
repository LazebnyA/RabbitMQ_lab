#!/usr/bin/env python
import pika
import random
import time
import sys
import os

MESSAGES_LIST = [
    "Service started successfully.",
    "Database connection established.",
    "User authentication successful.",
    "New order received.",
    "Payment processed successfully.",
    "Email notification sent to the user.",
    "Cache cleared successfully.",
    "Scheduled maintenance started.",
    "Backup completed successfully.",
    "Error: Unable to connect to the database.",
    "Warning: High memory usage detected.",
    "New user registered: user@example.com",
    "Job execution completed.",
    "Task added to the processing queue.",
    "System update applied successfully.",
    "Error: Invalid user input detected.",
    "Session expired for user: user@example.com",
    "Service stopped gracefully.",
    "Notification: Low disk space on server.",
    "Admin privileges granted to user: admin@example.com"
]

def main():
    counter = 0
    try:
        while True:
            # Generate a random sleep time between 1300ms and 2700ms
            time_to_sleep = random.randint(1300, 2700) / 1000  # Convert to seconds
            time.sleep(time_to_sleep)

            # Set up the connection to RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='localhost')
            )
            channel = connection.channel()

            # Declare the queue
            channel.queue_declare(
                queue='service-queue', 
                durable=False, 
                exclusive=False, 
                auto_delete=False, 
                arguments=None
            )

            # Prepare a random message from the list
            message = random.choice(MESSAGES_LIST)

            # Publish the message
            channel.basic_publish(exchange='',
                                  routing_key='service-queue',
                                  body=message)

            print(f"Message is sent into Default Exchange [N:{counter}] -> {message}")
            counter += 1

            # Close the connection
            connection.close()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

if __name__ == '__main__':
    main()
