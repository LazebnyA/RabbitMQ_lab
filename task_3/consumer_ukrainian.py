import pika

def callback(ch, method, properties, body):
    """
    Callback function to process received messages.
    """
    message = body.decode('utf-8')
    print(f"Received message: {message}")

def main():
    """
    Main function to consume messages from a RabbitMQ topic exchange.
    """
    connection_params = pika.ConnectionParameters('localhost')
    
    with pika.BlockingConnection(connection_params) as connection:
        channel = connection.channel()
        
        channel.exchange_declare(exchange="topic_logs", exchange_type="topic")
        
        result = channel.queue_declare('Ukrainian language queue', exclusive=True)
        queue_name = result.method.queue
        
        routing_key = "language.Ukrainian"
        channel.queue_bind(exchange="topic_logs", queue=queue_name, routing_key=routing_key)
        
        print(f"Subscribed to the queue '{queue_name}'")
        print(f"Listening to [{routing_key}]")
        
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        channel.start_consuming()

if __name__ == "__main__":
    main()
