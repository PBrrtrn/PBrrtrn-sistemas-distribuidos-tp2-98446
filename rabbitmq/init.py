import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.exchange_declare(exchange='stations_exchange', exchange_type='direct')

channel.queue_declare(queue='montreal_stations_queue', durable=False)
channel.queue_bind(exchange='stations_exchange',
                   queue='montreal_stations_queue',
                   routing_key='montreal')

channel.queue_declare(queue='other_stations_queue', durable=False)
channel.queue_bind(exchange='stations_exchange',
                   queue='other_stations_queue',
                   routing_key='toronto')

channel.queue_bind(exchange='stations_exchange',
                   queue='other_stations_queue',
                   routing_key='washington')
