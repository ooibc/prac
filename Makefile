.PHONY: build run-server tpc-local-test tpc ycsb local tmp msgtest exp
#include $(addr)
#  docker run -it --network host rac:v0
# docker run -it --name="c1" --network host rac:v0
# curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
# echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
# sudo apt-get update
  #
  # sudo apt-get install docker-ce docker-ce-cli containerd.io
# sudo docker pull lawyerphx/rac
# sudo docker run -dt --name="cohort" --network host lawyerphx/rac
# sudo docker run -it --name="collaborator" --network host lawyerphx/rac
# sudo docker exec -i cohort ./bin/rac-server -node=co -preload -addr=10.170.0.2:2001
# sudo docker exec -i collaborator make exp
# sudo docker exec -it cohort vim ./constants/constants.go
# sudo docker exec -it cohort make build
# gcloud compute scp --zone "asia-southeast2-a" "allvphx@coordinator" --recurse coordinator:~/rac/tmp ~/results

all: build local ycsb tpc

server:
	@gcloud beta compute ssh --zone "asia-southeast2-a" "allvphx@coordinator"  --project "primal-chariot-330902"
c0:
	@gcloud beta compute ssh --zone "asia-southeast1-a" "allvphx@cohort1"  --project "primal-chariot-330902"
c1:
	@gcloud beta compute ssh --zone "asia-east2-a" "allvphx@cohort2"  --project "primal-chariot-330902"
c2:
	@gcloud beta compute ssh --zone "asia-east1-a" "allvphx@cohort3"  --project "primal-chariot-330902"

clean:
	@docker rm $(docker ps -aq)
	@docker rmi $(docker images -aq)

pack:
	@docker build -t lawyerphx/rac:latest -f ./Dockerfile .
	@docker export -o rac.tar lawyerphx/rac:latest

build:
	@go build -o ./bin/rac-server ./rac-server/main.go

collaborator:
	@ssh allvphx@34.126.191.25

local:
	@tc qdisc add dev lo root handle 1: prio bands 4
	@tc qdisc add dev lo parent 1:4 handle 40: netem delay 20ms 2ms
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6001 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6002 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6003 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 5001 0xffff flowid 1:4

del:
	@tc filter del dev lo pref 4
	@tc qdisc  del dev lo root

### Just for test
micro-local-test:
	@go test -v ./experiment/speed_test.go -test.run TestMicro

test:
	@./bin/rac-server -node=ca -bench=tpc -addr=127.0.0.1:5001 -c=2000 -p=2pc

quick:
	@go run ./rac-server/main.go -local

tpc-local-test:
	@go test -v ./experiment/main_test.go ./experiment/main.go ./experiment/tpc.go -timeout 1h -test.run TestTPCCLocal

help:
	@echo "tpc 		----	run all tpc tests"
	@echo "ycsb 	----	run all ycsb tests"
	@echo "build 	----	build the binary for the rac-server"
	@echo "local	----	adapt Msg queue with filter on net card to introduce local message delay"
	@echo "serveri 	----	run cohort i, i = 0, 1, 2"

buildrpc:
	@cd downserver
	@python -m grpc_tools.protoc --python_out=. --grpc_python_out=. -I. rpc.proto
	@protoc --go_out=plugins=grpc:. rpc.proto

exp:
	@make build
	@python3 experiment/experiment.py

down:
	@python3 downserver/main.py 68
tt:
	@make build
	@./bin/rac-server -node=ca -addr=127.0.0.1:5001 -bench=tpc -p=rac -c=800 -r=2

tmp:
	@make build
	@./bin/rac-server -node=ca -addr=127.0.0.1:5001 --local -bench=tpc -p=3pc -c=800 -r=3

show:
	@tail -n 10 ./tmp/progress.log
