import json
import subprocess
import os
import time

pool = []
run_server = "sudo docker exec -i cohort ./bin/rac-server -node=co -preload -addr="
run_client_cmd  = "./bin/rac-server -node=ca -addr=127.0.0.1:5001"
protocols = ["rac", "3pc", "2pc"]

def get_client_cmd(bench, protocol, clients, r, file):
    return run_client_cmd + " -bench=" + str(bench) + \
           " -p=" + str(protocol) + \
           " -c=" + str(clients) + \
           " -r=" + str(r) + file

with open("./config.json") as f:
    config = json.load(f)

def execute_cmd_in_remote(host, cmd):
    cmd = "ssh " + "%s@%s" % ("allvphx", host) + " " + cmd
    #print(cmd)
    ssh = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    return ssh

def run_task(cmd):
    print(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         shell=True, preexec_fn=os.setsid)
    return p


def start_cohort(ext, service, r):
    ip = ext.split(":")[0]
    cmd = run_server + str(service) + " -r=" + str(r)
    return execute_cmd_in_remote(ip, cmd)

def start_service_on_all(r):
    for id_ in config["cohorts"]:
        pool.append(start_cohort(config["cohorts"][id_], config["cohorts"][id_], r))
    print("remote started")

def terminate_service():
    global pool
    for p in pool:
        p.wait()
    pool = []

def run_experiment(bench, r=3):
    upper = 800
    if bench == "tpc":
        upper += 500
    l = [c for c in range(50, upper+1, 50)]
    for c in l:
        filename = ">./tmp/%d/" % r + bench.upper() + str(c) + ".log"
        for po in protocols:
            rnd = 20
            for each in range(rnd):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd(bench, po, c, r, filename))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename

def run_crash(bench, c, r = 3):
    filename = ">./tmp/crash/" % r + bench.upper() + str(c) + ".log"
    for po in protocols:
        for each in range(5):
            start_service_on_all(r)
            time.sleep(2)
            p = run_task(get_client_cmd(bench, po, c, r, filename))
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename

def run_per(bench, c, r = 3):
    filename = ">./tmp/percent/" % r + bench.upper() + str(c) + ".log"
    for each in range(5):
        start_service_on_all(r)
        time.sleep(2)
        p = run_task(get_client_cmd(bench, "rac", c, r, filename))
        p.wait()
        terminate_service()
        if filename[1] == '.':
            filename = ">" + filename

if __name__ == '__main__':
    run_experiment("ycsb")
    run_experiment("tpc")