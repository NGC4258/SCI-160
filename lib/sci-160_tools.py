from subprocess import check_output
from subprocess import STDOUT
from subprocess import PIPE

class RSYNC(object):

    def __init__(self, myIP):
        self.cAdmin = "Please set cAdmin"
        clusterIPs = "Please set me up"
        self.ips = [ ip for ip in clusterIPs if ip != myIP ]

    def sync(self, hPath):
        resp = {}
        syncPath = hPath + "/tasks"
        try:
            for ip in self.ips:
                cmd = "rsync -au -e ssh {0} {1}@{2}:{3}".format(
                                                                syncPath,
                                                                self.cAdmin,
                                                                ip,
                                                                hPath
                )
                check_output(cmd, stderr=STDOUT, shell=True)
        except Exception as e:
            error = {
                     "title": "sync",
                     "message": e
            }
            resp.update({"error": error})
        else:
            resp.update({"code": "0"})
        return resp