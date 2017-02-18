#!/usr/bin/env python
import sys
import os
import signal
import time
import atexit
import logging
import threading
from subprocess import Popen
from subprocess import PIPE
from subprocess import call
from subprocess import check_output
from datetime import datetime
from datetime import timedelta
from lib.sci-160_redis import Register
from lib.sci-160_redis import Members
from lib.sci-160_redis import Tasks
from lib.sci-160_algorithm import Algorithm
from lib.sci-160_tools import RSYNC
from lib.sci-160_rest import PathDispatcher
from lib.sci-160_rest import MethodHandler
from wsgiref.simple_server import make_server 
from signal import SIGTERM 
    
class Housekeeper(object):
    """
    A generic daemon class.
    
    Usage: subclass the Daemon class and override the __run() method
    """
    def __init__(self, 
                 pidFile="/opt/sci-160/pid/sci-160.pid", 
                 logFile="/opt/sci-160/logs/sci-160.log", 
                 stdIn="/dev/null", 
                 stdOut="/dev/null", 
                 stdErr="/dev/null"):
        self.stdIn = stdIn
        self.stdOut = stdOut
        self.stdErr = stdErr
        self.pidFile = pidFile
        self.logFile = logFile
        self.hPath = os.path.split(os.path.realpath(__file__))[0]
        #Initial logging
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.logFile)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s[%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def __daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced 
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #1 failed: %d (%s)\n" % 
                             (e.errno, e.strerror)
            )
            sys.exit(1)
    
        # decouple from parent environment
        os.chdir("/") 
        os.setsid() 
        os.umask(0) 
    
        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #2 failed: %d (%s)\n" % 
                             (e.errno, e.strerror)
            )
            sys.exit(1) 
    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdIn, 'r')
        so = file(self.stdOut, 'a+')
        se = file(self.stdErr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
    
        # write pidfile
        atexit.register(self.__delpid)
        pid = str(os.getpid())
        file(self.pidFile,'w+').write("%s\n" % pid)
    
    def __delpid(self):
        os.remove(self.pidFile)

    def __start(self):
        self.logger.info("Start Housekeeper...")
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidFile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidFile)
            sys.exit(1)
        
        # Start the daemon
        self.__daemonize()
        self.__run()

    def __stop(self, internal=False):
        self.logger.info("Stop Housekeeper...")
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidFile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if not pid and not internal:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidFile)
            return # not an error in a restart

        if internal and os.pathexists(self.pidFile):
            self.__delpid()
        
        # Try killing the daemon process    
        try:
            os.kill(pid, SIGTERM)
            time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidFile):
                    self.__delpid()
            else:
                print str(err)
                sys.exit(1)

    def __restart(self):
        self.logger.info("Restart Housekeeper...")
        """
        Restart the daemon
        """
        self.__stop()
        time.sleep(2)
        self.__start()
            
    def __check_my_ip(self):
        try:
            cmd = ("ip a | grep inet | grep -v inet6 | awk '{print $2}' "
                   "| cut -d \"/\" -f 1"
            )
            ipaString = check_output(cmd, stderr=PIPE, shell=True)
            ipa = ipaString.strip().split("\n")
        except Exception as e:
            self.logger.debug(e)
            ipa = None
        if not ipa:
            self.logger.error("Cannot found IP address in this node.")
            self.__stop(internal=True)
        ips = "Please set me up"
        self.myIP = None
        for ip in ipa:
            if ip in ips:
                self.myIP = ip
                break
        if self.myIP:
            self.logger.info("Connection IP address: " + self.myIP)
        else:
            self.logger.error("Local IP for Redis was not found!")
            self.__stop(internal=True)

    def __register_to_redis(self):        
        try:
            rgtr = Register(self.myIP)
            task = Tasks(self.myIP)
        except Exception as e:
            self.__error_handle(
                {
                "error":{
                         "title": "__register_to_redis",
                         "message": e
                        }
                }
            )
            self.__stop(internal=True)
        else:
            startTime = datetime.today().strftime("%Y-%m-%d %H-%M-%S")
            resp = rgtr.register(self.myIP, startTime)
            if "error" in resp:
                self.__error_handle(resp)
                self.__stop(internal=True)
            else:
                self.logger.info("Redis registered")
            resp = task.set_change()
            if "error" in resp:
                self.__error_handle(resp)
            
    def __deregister_to_redis(self):
        try:
            rgtr = Register(self.myIP)
            task = Tasks(self.myIP)
        except Exception as e:
            self.__error_handle(
                {
                "error":{
                         "title": "__deregister_to_redis",
                         "message": e
                        }
                }
            )
        else:
            resp = rgtr.deregister(self.myIP)
            if "error" in resp:
                self.__error_handle(resp)
            else:
                self.logger.info("Redis deregistered")
            resp = task.set_change()
            if "error" in resp:
                self.__error_handle(resp)
            self.__clear_crontab()

    def __exit_gracefully(self, signum, frame):
        self.killNow = True

    def __error_handle(self, resp):
        title = resp["error"].get("title", "Unknow title")
        message = resp["error"]["message"]
        self.logger.error("Error: %s, %s" % (title, message))

    def __members_handler(self, mem, rgtr, task):

        def survival_of_me():
            now = datetime.today().strftime("%Y-%m-%d %H-%M-%S")
            resp = mem.update_survival_of_me(self.myIP, now)
            if "error" in resp:
                self.__error_handle(resp)
                return

        def survival_of_others():
            resp = mem.check_out_members()
            if "error" in resp:
                self.__error_handle(resp)
                return
            members = resp["members"]
            now = datetime.today()
            thirtySecAgo = now - timedelta(seconds=30)
            exceededTime = thirtySecAgo.strftime("%Y-%m-%d %H-%M-%S")
            resp = mem.check_survival_of_others(self.myIP, 
                                                     exceededTime,
                                                     members
            )
            self.logger.debug(resp)
            if "error" in resp:
                self.__error_handle(resp)
                return
            exceededList = resp["exceededList"]
            if len(exceededList) > 0:
                for i in exceededList:
                    resp = rgtr.deregister(i)
                    if "error" in resp:
                        self.__error_handle(resp)
                        return
                resp = task.set_change()
                if "error" in resp:
                    self.__error_handle(resp)

        def update_notice():
            notice = False
            resp = mem.check(self.myIP)
            if "error" in resp:
                self.__error_handle(resp)
                return
            new = resp["new"]
            leaved = resp["leaved"]
            members = resp["members"]
            if new:
                self.logger.info(("Joined to Housekeepers: "
                                 "{}").format(",".join(n for n in new))
                )
                notice = True
            if leaved:
                self.logger.info(("Leaved the Housekeeper: "
                                 "{}").format(",".join(l for l in leaved))
                )
                notice = True
            if notice:
                self.logger.info(("Members of Housekeeper: "
                                 "{}").format(",".join(m for m in members))
                )

        survival_of_me()
        survival_of_others()
        update_notice()

    def __task_allocation(self, task, algor):
        resp = task.get_the_change()
        self.logger.debug(resp)
        if "error" in resp:
            self.__error_handle(resp)
            return
        if int(resp.get("change")) == 0:
            return
        resp = task.get_nodes()
        self.logger.debug(resp)
        if "error" in resp:
            self.__error_handle(resp)
            return
        nodes = resp.get("nodes")
        resp = task.get_tasks()
        self.logger.debug(resp)
        if "error" in resp:
            self.__error_handle(resp)
            return
        tasks = resp.get("tasks")
        resp = task.set_unchange()
        self.logger.debug(resp)
        if "error" in resp:
            self.__error_handle(resp)
            return
        resp = algor.task_allocation(nodes, tasks)
        self.logger.debug(resp)
        if "error" in resp:
            self.__error_handle(resp)
            return
        allocation = resp["allocation"]
        resp = task.set_task_allocation(allocation)
        self.logger.debug(resp)
        if "error" in resp:
            self.__error_handle(resp)
            return
        resp = task.update_tasks_node(allocation)
        self.logger.debug(resp)
        if "error" in resp:
            self.__error_handle(resp)
            return
        resp = task.set_announcement(self.myIP)
        self.logger.debug(resp)
        if "error" in resp:
            self.__error_handle(resp)
            return

    def __task_announcement(self, task):

        def schedule_crontab():
            resp = task.get_task_allocation(self.myIP)
            if "error" in resp:
                self.__error_handle(resp)
                return
            if resp["allocation"].get("tasks"):
                taskIDs = resp["allocation"]["tasks"]
            else:
                taskIDs = []
            resp = task.get_tasks()
            if "error" in resp:
                self.__error_handle(resp)
                return
            tasks = resp.get("tasks")
            self.__schedule_crontab(taskIDs, tasks)

        resp = task.get_announcement(self.myIP)
        if "error" in resp:
            self.__error_handle(resp)
            return
        if str(resp["ip"]) != "0" and str(resp["ip"]) != self.myIP:
            schedule_crontab()
            self.logger.info("%s reallocated tasks." % resp["ip"])
        elif str(resp["ip"]) != "0":
            schedule_crontab()
            self.logger.info("reallocated tasks.")

    def __clear_crontab(self):
        try:
            call(["crontab", "-r"])
        except Exception as e:
            error = {
                     "title": "__clear_crontab",
                     "message": e
            }
            self.__error_handle({"error": error})

    def __schedule_crontab(self, taskIDs, tasks):

        def check_out_my_crons():
            t = []
            tApd = t.append
            housekeepingP = "/opt/sci-160/housekeeping"
            for i in tasks:
                for a in i.keys():
                    for taskID in taskIDs:
                        if a == taskID:
                            tApd("%s %s %s %s" % (i[a]["schedule"], 
                                                  housekeepingP, 
                                                  a,
                                                  self.myIP)
                            )
            return t

        try:
            crons = check_out_my_crons()
            if crons:
                cronStr = "\n".join(cron for cron in crons)
                p1 = Popen(["echo", cronStr], stdout=PIPE)
                p2 = Popen(["crontab"], stdin=p1.stdout, stdout=PIPE)
                p1.stdout.close()
            else:
                self.__clear_crontab()
        except Exception as e:
            error = {
                     "title": "__schedule_crontab",
                     "message": e
            }
            self.__error_handle({"error": error})

    def __sync_tasks(self, rsync):
        resp = rsync.sync(self.hPath)
        if "error" in resp:
            self.__error_handle(resp)
            return

    def __start_restful_api(self):
        try: 
            mh = MethodHandler(self.myIP, )
            dispatcher = PathDispatcher()
            dispatcher.register("GET", "/show", mh.show)
            dispatcher.register("POST", "/book", mh.book)
            dispatcher.register("POST", "/cancel", mh.cancel)
            dispatcher.register("PUT", "/switch", mh.switch)
            dispatcher.register("GET", "/epath", mh.executable_path)
            httpd = make_server("", 8444, dispatcher)
            restT = threading.Thread(target=httpd.serve_forever)
            restT.daemon = True
            restT.start()
        except Exception as e:
            error = {
                     "title": "__start_restful_api",
                     "message": e
            }
            self.logger.warning("Cannot open restful api: %s" % e)

    def __treading(self):
        try:
            task = Tasks(self.myIP)
            mem = Members(self.myIP)
            algor = Algorithm()
            rgtr = Register(self.myIP)
            rsync = RSYNC(self.myIP)
        except Exception as e:
            self.__error_handle(
                {
                "error":{
                         "title": "__threading",
                         "message": e
                        }
                }
            )
        else:
            self.__members_handler(mem, rgtr, task)
            self.__task_allocation(task, algor)
            self.__task_announcement(task)
            #self.__sync_tasks(rsync)

    def __run(self):
        self.__check_my_ip()
        self.__register_to_redis()
        self.logger.info("Running Housekeeper...")
        self.killNow = False
        signal.signal(signal.SIGTERM, self.__exit_gracefully)
        self.__start_restful_api()
        while 1:
            if self.killNow:
                self.__deregister_to_redis()
                break
            t = threading.Thread(target=self.__treading)
            t.daemon = True
            t.start()
            t.join(5)
            time.sleep(5)

    def controller(self, *action):
        if action[0] == "start":
            self.__start()
        if action[0] == "stop":
            self.__stop()
        if action[0] == "restart":
            self.__restart()

if __name__=="__main__":
    try:
        import setproctitle
        setproctitle.setproctitle("Housekeeper")
    except:
        print "Cannot open Housekeeper, error: importing setproctitle failed."
    else:
        de = Housekeeper()
        de.controller(*sys.argv[1:])
