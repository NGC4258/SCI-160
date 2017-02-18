#!/usr/bin/env python
import rediscluster
import sys
import json
import os
from datetime import datetime

def check_out_redis(port=7000):
    ip = "Please set me up"
    return [{"host": ip, "port": port}]

def check_id_exists(taskID):
    try:
        resp = r.hexists("_tasks", taskID)
    except Exception as e:
        print e
    finally:
        if resp:
            return True
    return False

def book(*info):

    def check_values():

        def check_schedule(schedule):
            if isinstance(schedule, basestring):
                t1 = schedule.split(" ")
                for t in t1:
                    if t != "*" and not unicode(t).isnumeric():
                        if "/" in t:
                            t2 = t.split("/")
                            for t in t2:
                                if t != "*" and not unicode(t).isnumeric():
                                    return True
                        else:
                            return True
                return False
            return True

        def check_timeout(timeout):
            if unicode(timeout).isnumeric() and int(timeout) <= 60:
                return False
            return True

        def check_path(executablePath):
            if isinstance(executablePath, basestring):
                if os.path.exists(executablePath):
                    return False
            return True

        if check_schedule(info[0]):
            print "%s is not correct." % info[0]
            sys.exit()
        if check_timeout(info[1]):
            print "%s is not correct." % info[1]
            sys.exit()
        if check_path(info[2]):
            print "%s is not correct." % info[2]
            sys.exit()

    def get_id_number():
        return datetime.now().strftime("%Y%m%d%H%M%S%f")

    check_values()
    redisValues = {
                   "executable_path": info[2],
                   "collection_name": "sci-160",
                   "schedule": info[0],
                   "timeout": info[1],
                   "node": "Not determined",
                   "switch": "on"
    }
    taskID = get_id_number()
    try:
        r.hset("_tasks", taskID, json.dumps(redisValues))
        r.hset("_tasks", "change", 1)
    except Exception as e:
        print e
    else:
        print "Task ID %s added." % taskID

def cancel(taskID):
    if not check_id_exists(taskID):
        print "Task ID %s not found!" % taskID
        sys.exit()
    try:
        r.hdel("_tasks", taskID)
        r.hset("_tasks", "change", 1)
    except Exception as e:
        print e
    else:
        print "Delete Task ID %s successful." % taskID

def switch(switch, taskID):
    if not check_id_exists(taskID):
        print "Task ID %s not found!" % taskID
        sys.exit()
    if switch != "on" and switch != "off":
        print "Please refer to the help for switch task."
        sys.exit()
    try:
        redisValues = json.loads(r.hget("_tasks", taskID))
        redisValues.update({"switch": switch})
        r.hset("_tasks", taskID, json.dumps(redisValues))
        r.hset("_tasks", "change", 1)
    except Exception as e:
        print e
    else:
        if switch == "off":
            print "%s has been turn off in Housekeeper." % taskID
        if switch == "on":
            print "%s has been turn on in Housekeeper." % taskID

def show():
    try:
        resp = r.hgetall("_tasks")
    except Exception as e:
        print e
    else:
        print ("{0:>13s} {1:>14s} {2:>8s} {3:>13s} {4:>10s} "
               "{5:>10s}").format("Task ID",
                                  "Switch",
                                  "Node",
                                  "Timeout",
                                  "Schedule",
                                  "Execution"
        )
        for taskID, values in resp.items():
            if taskID == "change":
                continue
            values = json.loads(values)
            print ("{0:>20s} {1:>5s} {2:>15s} {3:>6s} {4:>13s} "
                   "{5:<5s}").format(taskID,
                                      values["switch"],
                                      values["node"],
                                      values["timeout"],
                                      values["schedule"],
                                      values["executable_path"]
            )

def help():
    print "usage: python task.py [Action]"
    print "  book        Book a task to Housekeeper"
    print "  cancel      Cancel a task in Housekeeper"
    print "  switch      Turn on/off the task in Housekeeper"
    print "  show        Show the list of task from Housekeeper"
    print "  No Action   Show this help message and exit"
    print "  ------------------------------------------------"
    print "  book \"0 * * * *\" [timeout 0 - 60 mins] [executable path]"
    print "  cancel [task ID]"
    print "  switch [on/off] [task ID]"
    sys.exit()

if __name__=="__main__":
    if len(sys.argv) == 1:
        help()
    r = rediscluster.StrictRedisCluster(startup_nodes=check_out_redis())
    if sys.argv[1] == "book":
        book(*sys.argv[2:])
    if sys.argv[1] == "cancel":
        cancel(sys.argv[2])
    if sys.argv[1] == "switch":
        switch(sys.argv[2], sys.argv[3])
    if sys.argv[1] == "show":
        show()