from celery import Celery
from etl import *
import os


app = Celery("Celery App", broker=os.environ["REDIS_URL"])



@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    print("starting Copy of Data Order_Profile_array_tab")
    sender.add_periodic_task(
        60,  # seconds
        # an alternative to the @app.task decorator:
        # wrap the function in the app.task function
        update_odrerTable.s(),
        name="Update setup data",
    )


@app.task
def update_odrerTable():
    print("-------------->Starting to Update Order Profile Array Tab")
    main_etl()
    print("-------> End of Update")
