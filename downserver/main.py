import ql
from concurrent import futures
import time
import rpc_pb2
import grpc
import rpc_pb2_grpc

Train_Round = 50
Host = "localhost"
Port = 5003

class Learner:
    def __init__(self):
        self.mo = ql.init()
        self.eps = 0.3
        self.states = 0
        self.st = -1
        self.act = 0
        self.level = -1

    # about 10 : 3 : 4 : 6
    def get_reward(self, level):
        if level > self.level and self.act == 0:
            return -7
        elif level == -1:
            return 0
        elif level == 0:
            return -1
        else:
            return -3

    def reset(self, level):
        self.eps = max(0.0, self.eps -  0.3 / Train_Round)
        if self.eps > 0:
            if self.st != -1:
                self.mo.update_transition(self.st, self.act, self.get_reward(level), self.states,
                                             self.states == 6)
        self.st = -1
        self.states = 0

    def action(self, level):
        self.eps = max(0.0, self.eps -  0.3 / Train_Round)
        if self.eps > 0:
            if self.st != -1:
                self.mo.update_transition(self.st, self.act, self.get_reward(level), self.states,
                                             self.states == 6)
            self.act = self.mo.choose_action(self.states, self.eps)
            self.st = self.states
        else:
            self.act = self.mo.max_q(self.states)[0]

        if self.act == 0:
            self.states = 6
            self.reset(level)
        else:
            self.states += 1
        return self.act

Learners = {
    "1.1" : Learner(),
    "1.2" : Learner(),
    "2.1" : Learner(),
    "2.2" : Learner(),
    "3.1" : Learner(),
    "3.2" : Learner(),
}

class Action(rpc_pb2_grpc.ActionServicer):
    def action(self, request, context):
        lev = request.level - 1
        i = str(int(request.cid[-1]) - int('0'))
#        if i == "1":
#            print(lev + 1)
        if lev == 0:
            Learners[i+".1"].reset(lev)
            Learners[i+".2"].reset(lev)
            return rpc_pb2.Act(action = 1)
        else:
            i += "." + str(lev)
#            print("ips = ", i)
            res = Learners[i].action(lev)
#            print("act = ", res)
            return rpc_pb2.Act(action = res)

class Reset(rpc_pb2_grpc.ResetServicer):
    def reset(self, request, context):
        lev = request.level - 1
        i = str(int(request.cid[-1]) - int('0'))
        Learners[i+".1"].reset(lev)
        Learners[i+".2"].reset(lev)
        return rpc_pb2.Act(action = 1)

if __name__ == '__main__':
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    rpc_pb2_grpc.add_ResetServicer_to_server(Reset(), server)
    rpc_pb2_grpc.add_ActionServicer_to_server(Action(), server)

    server.add_insecure_port('0.0.0.0:5003')
    server.start()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        server.stop(0)