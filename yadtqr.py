import uuid
import json
from kafka import KafkaProducer, KafkaConsumer
import redis

class YADTQ:
    def __init__(self, broker, redis_host='localhost', redis_port=6379):
        self.broker = broker
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Connect to Redis
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    def send_task(self, task_name, args):
        task_id = str(uuid.uuid4())
        task = {
            "task-id": task_id,
            "task": task_name,
            "args": args
        }
        
        # Store initial status in Redis
        self.redis_client.hset(task_id, "status", "queued")
        print(f"Task {task_id} status: queued")

        # Send task to Kafka
        try:
            self.producer.send('task_queue', value=task)
            self.producer.flush()
        except Exception as e:
            print(f"Error sending task {task_id}: {e}")
        return task_id

    def update_status(self, task_id, status):
        self.redis_client.hset(task_id, "status", status)
        print(f"Task {task_id} status: {status}")

    def print_result(self, task_id, result=None):
        if result is not None:
            self.redis_client.hset(task_id, "result", result)
            print(f"Task {task_id} completed with result: {result}")
        else:
            print(f"Task {task_id} failed")

    def start_consumer(self):
        consumer = KafkaConsumer(
            'task_results',
            bootstrap_servers=self.broker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("Starting consumer for task_results...")
        try:
            for message in consumer:
                task_result = message.value
                task_id = task_result["task-id"]
                status = task_result["status"]
                self.update_status(task_id, status)
                if status == "success":
                    self.print_result(task_id, task_result.get("result"))
                elif status == "failed":
                    self.print_result(task_id)
        except Exception as e:
            print(f"Error in consumer: {e}")

