from .redis_client import r
from celery import shared_task

import time
from datetime import datetime


from .utils import send_mail


@shared_task
def add(x, y):
    print(f"Adding {x} + {y}")
    return x + y


@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3},
    retry_backoff=2,
)
def divide(x, y):
    return x / y

attempts = 0

@shared_task(
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3},
    retry_backoff=2,
)
def unstable_task():
    global attempts

    attempts += 1

    print(f"Attempt {attempts}")

    if attempts < 3:
        raise Exception("Temporary failure")

    return "Success!"


@shared_task
def process_order(order_id):
    print(f"Processing order {order_id}")
    return f"Order {order_id} processed"


@shared_task
def send_welcome_email(user_id):
    key = f"email_sent:{user_id}"

    # Check Redis
    if r.get(key):
        return "Already processed"

    # Do the side effect
    send_mail(user_id)

    # Mark as completed
    r.set(key, "1")


@shared_task
def send_welcome_email_with_nx(user_id):
    key = f"email_sent:{user_id}"

    acquired = r.set(
        key,
        "1",
        nx=True,
        ex=300
    )

    if not acquired:
        return "Already processing/processed"

    send_mail(user_id)


@shared_task
def heartbeat():
    print(f"Heartbeat: {datetime.now()}")


@shared_task
def process_document(document_id, duration):
    print(f"START document {document_id} — taking {duration}s")

    time.sleep(duration)

    print(f"FINISH document {document_id}")

    return f"Document {document_id} processed"


@shared_task(queue="slow")
def slow_task(task_id):
    print(f"START SLOW {task_id}")
    time.sleep(10)
    print(f"FINISH SLOW {task_id}")
    return f"Slow {task_id} done"


@shared_task(queue="fast")
def fast_task(task_id):
    print(f"START FAST {task_id}")
    time.sleep(1)
    print(f"FINISH FAST {task_id}")
    return f"Fast {task_id} done"