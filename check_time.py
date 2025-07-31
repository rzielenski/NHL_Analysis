from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time
import logging

# Enable logging
logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

def test_job():
    print(f"[{datetime.utcnow()}] ✅ TEST JOB RAN SUCCESSFULLY")

scheduler = BackgroundScheduler()
scheduler.start()

scheduler.add_job(
    test_job,
    trigger='interval',
    seconds=5,
    id='test_interval',
    replace_existing=True
)

print(f"[{datetime.utcnow()}] Scheduled job. Waiting...")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    scheduler.shutdown()
