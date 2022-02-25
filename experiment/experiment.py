import json
import subprocess
import os
import time

local = False
pool = []
run_server = "sudo docker exec -i cohort ./bin/rac-server -node=co -preload -addr="
run_rl_server_cmd = "python3 downserver/main.py "
protocols = ["rac", "3pc", "2pc"]
logf = open("./tmp/progress.log", "w")

logf = open("./tmp/progress.log", "w")

if local:
    run_client_cmd  = "./bin/rac-server -node=ca -local=true -addr="
else:
    run_client_cmd  = "./bin/rac-server -node=ca -addr="

def get_server_cmd(addr, r, minlevel, env, nf):
    cmd = run_server + str(addr) + \
          " -r=" + str(r) + \
          " -tl=" + str(env) + \
          " -nf=" + str(nf) + \
          " -ml=" + str(minlevel)
    return cmd

def get_client_cmd(bench, protocol, clients, r, file, env=20, alg=1, nf=-1, ml = 1):
    return run_client_cmd + " -bench=" + str(bench) + \
           " -p=" + str(protocol) + \
           " -c=" + str(clients) + \
           " -d=" + str(alg) + \
           " -nf=" + str(nf) + \
           " -tl=" + str(env) + \
           " -ml=" + str(ml) + \
           " -r=" + str(r) + file

if local:
    with open("./configs/local.json") as f:
        config = json.load(f)
else:
    with open("./configs/remote.json") as f:
        config = json.load(f)


for id_ in config["collaborators"]:
    run_client_cmd = run_client_cmd + config["collaborators"][id_]

# gcloud beta compute ssh --zone "asia-southeast1-a" "cohort1" -- '
def execute_cmd_in_gcloud(zone, instance, cmd):
    cmd = "gcloud beta compute ssh --zone " + "%s %s -- \'" % (zone, instance) + " " + cmd + "\'"
    ssh = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    return ssh

def run_task(cmd):
    print(cmd, file=logf)
    logf.flush()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         shell=True, preexec_fn=os.setsid)
    return p


def start_cohort(zone, instance, service, r, minlevel, env, nf):
    cmd = get_server_cmd(service, r, minlevel, env, nf)
    return execute_cmd_in_gcloud(zone, instance, cmd)

def start_service_on_all(r, run_rl = False, time = 0, minlevel=1, env=25, nf=-1):
    if run_rl:
        pool.append(run_task(run_rl_server_cmd + str(time) + ">./tmp/train.log"))
    if local:
        return
    for id_ in config["cohorts"]:
        pool.append(start_cohort(config["zones"][id_], config["instances"][id_], config["cohorts"][id_], r, minlevel, env, nf))

def terminate_service():
    global pool
    for p in pool:
        p.wait()
    pool = []

TestBatch = 10

def delete_extra_zero(n):
    if isinstance(n, int):
        return str(n)
    if isinstance(n, float):
        n = str(n).rstrip('0')
        if n.endswith('.'):
            n = n.rstrip('.')
        return n
    return "nooo"

def run_exp_dense(bench, r=3, proto = "all"):
    upper = 1000
    l = [c for c in range(50, upper+1, 50)]
    for c in l:
        filename = ">./tmp/" + delete_extra_zero(r) + "/" + bench.upper() + str(c) + ".log"
        if proto == "all":
            for po in protocols:
                for each in range(TestBatch):
                    start_service_on_all(r)
                    time.sleep(1)
                    p = run_task(get_client_cmd(bench, po, c, r, filename))
                    p.wait()
                    terminate_service()
                    if filename[1] == '.':
                        filename = ">" + filename
            for each in range(TestBatch):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd(bench, "rac", c, r, filename, ml=2))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename
        else:
            for each in range(TestBatch):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd(bench, proto, c, r, filename))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename

def run_heu(alg, env, bench = "tpc", c = 700, r = 4.5, nf = -1):
    if env <= 0:
        filename = ">./tmp/he/CF-" + str(-env)  + "-" + str(alg) + ".log"
    else:
        filename = ">./tmp/he/NF-" + str(nf)  + "-" + str(alg) + ".log"

    for each in range(TestBatch):
        start_service_on_all(r, run_rl= (alg == 0), time=3 * max(-env, nf) + 5 + 5, env=env, nf=nf)
        time.sleep(2)
        p = run_task(get_client_cmd(bench, "rac", c, r, filename, env, alg, nf))
        p.wait()
        terminate_service()
        if filename[1] == '.':
            filename = ">" + filename

def run_exp_loose(bench, r):
    l = [2**c for c in range(0, 11)]
    for c in l:
        filename = ">./tmp/loose/" + bench.upper() + str(c) + ".log"
        rnd = TestBatch

        for po in ["pac"]:
            for each in range(rnd):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd(bench, po, c, r, filename))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename

def run_loose_heu():
    for t in [1, 4, 16]:
        for i in [1, 2, 4, 8, 16, 32, 64, 128]: #, 3, 4, 5, 6, 7, 8, 12, 16, 24, 32, 48, 64, 96, 128]:
            run_heu(i, -t)
            run_heu(i, 33, nf=t)

def run_all_heu():
    t = 1
    for r in range(5):
        for i in [0, 1, 2 , 3, 4, 5, 6, 7, 8, 12, 16, 24, 32, 48, 64, 96, 128]:
            run_heu(i, -t)
        t *= 2

    t = 1
    for r in range(5):
        for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 24, 32, 48, 64, 96, 128]:
            run_heu(i, 33, nf=t)
        t *= 2

def run_loose_heu():
    for t in [1, 16]:
        for i in [1, 2, 4, 8, 16, 32, 64, 128]: #, 3, 4, 5, 6, 7, 8, 12, 16, 24, 32, 48, 64, 96, 128]:
            run_heu(i, -t)
            run_heu(i, 33, nf=t)

if __name__ == '__main__':
#    run_exp_dense("tpc", 3)
#    run_exp_dense("ycsb", 3) needs to change constants
#    for r in range(1, 3):
#        run_exp_dense("tpc", r, "rac")
#    for r in range(4, 8):
#        run_exp_dense("tpc", r, "rac")
    run_exp_loose("ycsb", 3)
#    run_exp_loose("ycsb", 3, "tpc")
#    run_exp_loose("ycsb", 3, "tpc")
#    for r in [0.5, 1.3, 1.6, 2.5, 3.5, 4.5, 5.5, 7, 8]:
#        run_exp_dense("tpc", r, "rac")
    run_loose_heu()
#    logf.close()
