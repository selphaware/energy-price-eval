import logging
import datetime

# set up logger

date = "{:%Y-%m-%d}".format(datetime.datetime.now())
log_file_string = "../logs/{}.log".format(date)

logging.basicConfig(filename=log_file_string, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s: %(message)s",
                    datefmt="%Y/%m/%d %I:%M:%S %p")

logger = logging
