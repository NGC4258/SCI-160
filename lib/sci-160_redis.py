import rediscluster
import json

class Connection(object):

    def __init__(self, redisIP="127.0.0.1", redisPort="7000"):
        startupN = [{"host": redisIP, "port": redisPort}]
        try:
            self.r = rediscluster.StrictRedisCluster(startup_nodes=startupN)
        except Exception as e:
            self.rErr = e
        else:
            self.rErr = False

class Register(Connection):

    def __init__(self, ip):
        super(Register, self).__init__(ip)

    def register(self, ip, time):
        resp = {}
        if self.rErr:
            error = {
                     "title": "register",
                     "message": self.rErr
            }
            resp.update({"error": error})
            return resp
        try:
            members = self.r.hkeys("_register")
            for m in members:
                if m != ip:
                    self.r.hset(m, ip, "new")
            self.r.hset("_register", ip, 1)
            self.r.hset(ip, "on", time)
        except Exception as e:
            error = {
                     "title": "register",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": 0})
        return resp

    def deregister(self, ip):
        resp = {}
        if self.rErr:
            error = {
                     "title": "deregister",
                     "message": self.rErr
            }
            resp.update({"error": error})
            return resp
        try:
            members = self.r.hkeys("_register")
            for m in members:
                if m != ip:
                    self.r.hset(m, ip, "leaved")
            self.r.hdel("_register", ip)
            self.r.hdel(ip, "on")
        except Exception as e:
            error = {
                     "title": "deregister",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": 0})
        return resp

class Members(Connection):

    def __init__(self, ip):
        super(Members, self).__init__(ip)

    def check_out_members(self):
        resp = {}
        try:
            members = self.r.hkeys("_register")
        except Exception as e:
            error = {
                     "title": "check_out_members",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"members": members})
        return resp

    def check(self, myIP):
        resp = {}
        try:
            members = self.r.hkeys("_register")
            newMem = self.__check_new_member(myIP, members)
            myBoard = self.r.hkeys(myIP)
            leavedMem = self.__check_leaved_member(myIP, myBoard)
        except Exception as e:
            error = {
                     "title": "check",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({
                         "new": newMem,
                         "leaved": leavedMem,
                         "members": members
                        }
            )
        return resp

    def update_survival_of_me(self, myIP, time):
        resp = {}
        try:
            self.r.hset(myIP, "on", time)
        except Exception as e:
            error = {
                     "title": "update_survival_of_me",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0"})
        return resp

    def check_survival_of_others(self, myIP, exceededTime, members):
        resp = {}
        exceededList = []
        try:
            for member in members:
                if member == myIP:
                    continue
                memberTime = self.r.hget(member, "on")
                if exceededTime > memberTime:
                    exceededList.append(member)
        except Exception as e:
            error = {
                     "title": "check_survival_of_others",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0", "exceededList": exceededList})
        return resp

    def __check_new_member(self, myIP, members):
        newMem = []
        for m in members:
            status = self.r.hget(myIP, m)
            if status == "new":
                newMem.append(m)
                self.r.hdel(myIP, m)
        return newMem

    def __check_leaved_member(self, myIP, myBoard):
        leavedMem = []
        for b in myBoard:
            if b == "on":
                continue
            status = self.r.hget(myIP, b)
            if status == "leaved":
                leavedMem.append(b)
                self.r.hdel(myIP, b)
        return leavedMem

class Tasks(Connection):

    def __init__(self, ip):
        super(Tasks, self).__init__(ip)

    def get_the_change(self):
        resp = {}
        try:
            change = self.r.hget("_tasks", "change")
        except Exception as e:
            error = {
                     "title": "get_the_change",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0", "change": change})
        return resp

    def get_nodes(self):
        resp = {}
        try:
            nodes = self.r.hkeys("_register")
        except Exception as e:
            error = {
                     "title": "get_nodes",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0", "nodes": nodes})
        return resp

    def get_tasks(self):
        resp = {}
        temp = []
        tempApd = temp.append
        try:
            tasks = self.r.hgetall("_tasks")
            for k, v in tasks.items():
                if k == "change":
                    continue
                v = json.loads(v)
                if v["switch"] == "off":
                    continue
                tempApd({k: v})
        except Exception as e:
            error = {
                     "title": "get_tasks",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0", "tasks": temp})
        return resp

    def set_change(self):
        resp = {}
        try:
            self.r.hset("_tasks", "change", 1)
        except Exception as e:
            error = {
                     "title": "set_change",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0"})
        return resp

    def set_unchange(self):
        resp = {}
        try:
            self.r.hset("_tasks", "change", 0)
        except Exception as e:
            error = {
                     "title": "set_unchange",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0"})
        return resp

    def set_announcement(self, myIP):
        resp = {}
        try:
            targets = self.r.hkeys("_register")
            for i in targets:
                self.r.hset(i, "tasks_changed", myIP)
        except Exception as e:
            error = {
                     "title": "changed_announcement",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0"})
        return resp

    def get_announcement(self, myIP):
        resp = {}
        try:
            msg = self.r.hget(myIP, "tasks_changed")
            if not msg:
                msg = "0"
            self.r.hset(myIP, "tasks_changed", "0")
        except Exception as e:
            error = {
                     "title": "get_announcement",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0", "ip": msg})
        return resp

    def set_task_allocation(self, allocation):
        resp = {}
        try:
            nodes = self.r.hkeys("_allocation")
            for node in nodes:
                self.r.hdel("_allocation", node)
            for k, v in allocation.items():
                self.r.hset("_allocation", k, json.dumps(v))
        except Exception as e:
            error = {
                     "title": "set_task_allocation",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0"})
        return resp

    def update_tasks_node(self, allocation):
        resp = {}
        try:
            temp = {}
            for k, v in allocation.items():
                for i in v.get("tasks", []):
                    temp.update({i: k})
            tasks = self.r.hgetall("_tasks")
            for taskID, info in tasks.items():
                if taskID == "change":
                        continue
                info = json.loads(info)
                if taskID in temp.keys():
                    info.update({"node": temp[taskID]})
                else:
                    info.update({"node": "Not determined"})
                self.r.hset("_tasks", taskID, json.dumps(info))
        except Exception as e:
            error = {
                     "title": "update_tasks_node",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0"})
        return resp

    def get_task_allocation(self, myIP):
        resp = {}
        try:
            allocation = self.r.hget("_allocation", myIP)
        except Exception as e:
            error = {
                     "title": "get_task_allocation",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0", "allocation": json.loads(allocation)})
        return resp

class Housekeeping(Connection):

    def __init__(self, ip):
        super(Housekeeping, self).__init__(ip)

    def get_the_information(self, taskID):
        resp = {}
        try:
            info = self.r.hget("_tasks", taskID)
        except Exception as e:
            error = {
                     "title": "get_the_information",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0", "info": json.loads(info)})
        return resp