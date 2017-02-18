class Algorithm(object):

    def __init__(self):
        pass

    def task_allocation(self, nodes, tasks):
        resp = {}

        def set_loadings(tS):
            tW = {}
            tWUpd = tW.update
            for i in tS:
                for a in i.keys():
                    timeout = i[a]["timeout"]
                    schedule = i[a]["schedule"]
                    loadingA = 2
                    if timeout > 5:
                        loadingA = 4
                    if timeout > 30 or timeout == 0:
                        loadingA = 8
                    sL = schedule.split(" ")
                    m, h, d, M, w = sL[0], sL[1], sL[2], sL[3], sL[4]
                    loadingB = 2
                    if M == "*" and w == "*" and d == "*":
                        loadingB = 8
                        if m != "*" and h == "*":
                            loadingB = 16
                    tWUpd({a: (loadingA + loadingB) / 2})
            return tW

        def count_total_loadings(tW):
            cTW = 0
            for v in tW.values():
                cTW = cTW + v
            return cTW

        def average_loadings(cTW, cNS):
            if cNS == 0:
                return 0
            return int(cTW / cNS)

        def allocate_tasks_by_loadings(tW, nS):
            temp = {}
            tempUpd = temp.update
            for n in nS:
                tempUpd({n: {}})
            for taskID, taskW in tW.items():
                aC = 9223372036854775807
                aW = 9223372036854775807
                for n in nodes:
                    #find the minimum tasks of node
                    if temp[n].get("tCount", 0) <= aC:
                        aC = temp[n].get("tCount", 0)
                        mtn = n
                    #find the least loadings of node
                    if temp[n].get("loadings", 0) <= aW:
                        aW = temp[n].get("loadings", 0)
                        lwn = n
                if temp[lwn].get("loadings", 0) < averageLoadings:
                    if not temp[lwn].get("tasks"):
                        tasks = [taskID]
                    else:
                        tasks = temp[lwn]["tasks"]
                        tasks.append(taskID)
                    info = {
                            "tCount": temp[lwn].get("tCount", 0) + 1,
                            "loadings": temp[lwn].get("loadings", 0) + taskW,
                            "tasks": tasks
                    }
                    tempUpd({lwn: info})
                else:
                    if not temp[mtn].get("tasks"):
                        tasks = [taskID]
                    else:
                        tasks = temp[mtn]["tasks"]
                        tasks.append(taskID)
                    info = {
                            "tCount": temp[mtn].get("tCount", 0) + 1,
                            "loadings": temp[mtn].get("loadings", 0) + taskW,
                            "tasks": tasks
                    }
                    tempUpd({mtn: info})
            return temp

        try:
            tasksLoading = set_loadings(tasks)
        except Exception as e:
            error = {
                     "title": "task_allocation.set_loadings",
                     "message": e
            }
            resp.update({"error": error})
            return resp
        try:
            totalLoadings = count_total_loadings(tasksLoading)
        except Exception as e:
            error = {
                     "title": "task_allocation.count_total_loadings",
                     "message": e
            }
            resp.update({"error": error})
            return resp
        try:
            averageLoadings = average_loadings(totalLoadings, len(nodes))
        except Exception as e:
            error = {
                     "title": "task_allocation.average_loadings",
                     "message": e
            }
            resp.update({"error": error})
            return resp
        try:          
            resp.update({
                "code": 0,
                "allocation": allocate_tasks_by_loadings(tasksLoading, nodes)
                        }
            )
        except Exception as e:
            error = {
                     "title": "task_allocation.allocate_tasks_by_loadings",
                     "message": e
            }
            resp.update({"error": error}) 
        return resp
