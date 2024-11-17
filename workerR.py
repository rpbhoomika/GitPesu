import json
import time
import redis
from kafka import KafkaConsumer, KafkaProducer

# Define task functions
def add(a, b):
    return a + b

def sub(a, b):
    return a - b

def multiply(a, b):
    return a * b

# Configure Kafka and Redis
broker = 'localhost:9092'
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Kafka Consumer and Producer configuration
consumer = KafkaConsumer(
    'task_queue',
    bootstrap_servers=broker,
    group_id='worker-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)
producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Worker started and listening for tasks...")

for message in consumer:
    task = message.value
    task_id = task["task-id"]
    task_name = task["task"]
    args = task["args"]

    # Set task status to processing in Redis
    redis_client.hset(task_id, "status", "processing")
    print(f"Task {task_id} status: processing")

    try:
        # Execute the specified task
        if task_name == "add":
            result = add(*args)
        elif task_name == "sub":
            result = sub(*args)
        elif task_name == "multiply":
            result = multiply(*args)
        else:
            raise ValueError("Unknown task")

        # Simulate processing time
        time.sleep(2)

        # Send success result back to Kafka and update Redis
        result_message = {
            "task-id": task_id,
            "status": "success",
            "result": result
        }
        producer.send('task_results', result_message)
        redis_client.hset(task_id, "status", "success")
        redis_client.hset(task_id, "result", result)
        print(f"Task {task_id} status: success, Result: {result}")

    except Exception as e:
        # Handle task failure
        error_message = {
            "task-id": task_id,
            "status": "failed",
            "error": str(e)
        }
        producer.send('task_results', error_message)
        redis_client.hset(task_id, "status", "failed")
        redis_client.hset(task_id, "error", str(e))
        print(f"Task {task_id} status: failed, Error: {e}")

