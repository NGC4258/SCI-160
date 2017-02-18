#!/usr/bin/env python
import subprocess
import sys
import logging
import time
from time import localtime
from time import strftime
from lib.housekeeper_redis import Housekeeping

def get_info(taskID, myIP):
    housekeeping = Housekeeping(myIP)
    info = housekeeping.get_the_information(taskID)
    return info

def get_timeout(timeoutMin):
    if timeoutMin == 0:
        return 86400
    timeoutSec = timeoutMin * 60
    if timeoutSec > 3600:
        return 3600
    return timeoutSec

def main(taskID, myIP, logFile="/opt/sci-160/logs/sci-160.log"):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(logFile)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter(("%(asctime)s[%(levelname)s] "
                                   "Housekeeping: %(message)s")
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    startTime = time.time()
    stTime = strftime("%a, %d %b %Y %H:%M:%S", localtime())
    logger.info("Start task: %s at %s" % (taskID, stTime))
    resp = get_info(taskID, myIP)
    if "error" in resp:
        logger.error(resp["error"])
        return
    info = resp["info"]
    p = subprocess.Popen([info["executable_path"]],
                         stdout=subprocess.PIPE, 
                         stderr=subprocess.PIPE
    )
    runningTime = get_timeout(info["timeout"])
    while runningTime > 0:
        if p.poll:
            break
        time.sleep(1)
        runningTime = runningTime - 1
    out, err = p.communicate()
    if err:
        logger.error("task: %s, %s" % (taskID, err))
    if not p.poll:
        p.terminate()
    if not p.poll:
        p.kill()
    stopTime = time.time()
    runTime = round(stopTime - startTime, 2)
    stpTime = strftime("%a, %d %b %Y %H:%M:%S", localtime())
    logger.info("Stop task: %s at %s, taken: %s" % (taskID, stpTime, runTime))

if __name__=="__main__":
    main(sys.argv[1], sys.argv[2])