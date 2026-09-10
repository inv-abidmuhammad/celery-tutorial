from .redis_client import r
from celery import shared_task

from .utils import send_email


@shared_task
def add(x, y):
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
    send_email(user_id)

    # Mark as completed
    r.set(key, "1")