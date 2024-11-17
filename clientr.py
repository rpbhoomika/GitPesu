from yadtq import YADTQ
import threading
import redis

# Configure Redis client
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Configure YADTQ with Kafka broker
yadtq_client = YADTQ(broker='localhost:9092')

# Start a separate thread to consume results
consumer_thread = threading.Thread(target=yadtq_client.start_consumer)
consumer_thread.daemon = True
consumer_thread.start()

# Send multiple tasks and print their task IDs
task_ids = [
    yadtq_client.send_task("add", args=[1, 2]),
    yadtq_client.send_task("sub", args=[3, 2]),
    yadtq_client.send_task("multiply", args=[16, 2]),
]

# Optionally, check task status and results in Redis
for task_id in task_ids:
    print(f"\nChecking status in Redis for Task {task_id}:")
    status = redis_client.hget(task_id, "status")
    result = redis_client.hget(task_id, "result")
    if status:
        print(f"Task {task_id} Status: {status}")
    if result:
        print(f"Task {task_id} Result: {result}")
    else:
        print(f"Task {task_id} Result not available yet.")

# Wait for all tasks to be processed
consumer_thread.join()

