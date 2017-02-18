import time
import rediscluster
import sys
import json
import os
from datetime import datetime

def notfound_404(environ, start_response):
    msg = b"Welcome to the restful API for DB management."
    start_response("404 Not Found", [ ("Content-type", "text/plain")])
    return msg

class PathDispatcher(object):

    def __init__(self):
        self.pathmap = { }

    def __call__(self, environ, start_response):
        path = environ["PATH_INFO"]
        method = environ["REQUEST_METHOD"].lower()
        try:
            requestBodySize = int(environ.get('CONTENT_LENGTH', 0))
        except (ValueError):
            requestBodySize = 0
        requestBody = environ['wsgi.input'].read(requestBodySize)
        if requestBodySize == 0:
            environ["params"] = None
        else: 
            environ["params"] = json.loads(requestBody)
        handler = self.pathmap.get((method, path), notfound_404)
        return handler(environ, start_response)

    def register(self, method, path, function):
        self.pathmap[method.lower(), path] = function
        return function

class MethodHandler(object):

    def __init__(self, ip, port=7000):
        node = [{"host": ip, "port": port}]
        self.r = rediscluster.StrictRedisCluster(startup_nodes=node)

    def show(self, environ, start_response):
        resp = {}
        respUpd = resp.update
        start_response("200 OK", [ ("Content-type", "application/json") ])
        try:
            tasks = self.r.hgetall("_tasks")
        except Exception as e:
            return str(e).encode("utf-8")
        else:
            for taskID, values in tasks.items():
                if taskID == "change":
                    continue
                values = json.loads(values)
                respUpd({taskID: values})
        return json.dumps({"tasks": resp})

    def book(self, environ, start_response):
        resp = {}
        start_response("200 OK", [ ("Content-type", "application/json") ])
        params = environ["params"]
        taskID = datetime.now().strftime("%Y%m%d%H%M%S%f")
        try:
            redisValues = {
                           "executable_path": params["executable_path"],
                           "collection_name": "sci-160",
                           "schedule": params["schedule"],
                           "timeout": params["timeout"],
                           "node": "Not determined",
                           "switch": "on"
            }
            self.r.hset("_tasks", taskID, json.dumps(redisValues))
            self.r.hset("_tasks", "change", 1)
        except Exception as e:
            error = {
                     "title": "book",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": 0, "task_id": taskID})
        return json.dumps({"book": resp})            

    def cancel(self, environ, start_response):
        resp = {}
        start_response("200 OK", [ ("Content-type", "application/json") ])
        params = environ["params"]
        try:
            self.r.hdel("_tasks", params["task_id"])
            self.r.hset("_tasks", "change", 1)
        except Exception as e:
            error = {
                     "title": "cancel",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": 0})
        return json.dumps({"cancel": resp})

    def switch(self, environ, start_response):

        def check_id_exists(taskID):
            try:
                resp = self.r.hexists("_tasks", taskID)
            except Exception as e:
                return False
            finally:
                if resp:
                    return True
            return False

        resp = {}
        start_response("200 OK", [ ("Content-type", "application/json") ])
        params = environ["params"]
        if not check_id_exists(params["task_id"]):
            error = {
                     "title": "switch",
                     "message": "task id not found!"
            }
            resp.update({"error": error})
            return json.dumps({"switch": resp})
        try:
            redisValues = json.loads(self.r.hget("_tasks", params["task_id"]))
            redisValues.update({"switch": params["switch"]})
            self.r.hset("_tasks", params["task_id"], json.dumps(redisValues))
            self.r.hset("_tasks", "change", 1)
        except Exception as e:
            error = {
                     "title": "switch",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": 0})
        return json.dumps({"switch": resp})

    def executable_path(self, environ, start_response):
        taskPath = "/opt/sci-160/tasks"
        executablePath = []
        executablePathApd = executablePath.append
        start_response("200 OK", [ ("Content-type", "application/json") ])
        try:
            for dirPath, dirNames, fileNames in os.walk(taskPath):
                for f in fileNames:
                    fPath = os.path.join(dirPath, f)
                    mask = str(oct(os.stat(fPath).st_mode))[4:]
                    if "1" in mask or "5" in mask or "7" in mask:
                        executablePathApd(fPath)
        except Exception as e:
            return str(e).encode("utf-8")
        return json.dumps({"executable_path": executablePath})
