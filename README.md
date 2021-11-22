## RAC

A distributed kv-store with adaptive atomic commit protocol supported.

It supports

- 2PC, 3PC, and RAC ACPs.
- TPC-C, and Micro-benchmark Test.



#### Installation

Firstly, the docker container could be pulled from the DockerHub.

```shell
sudo docker pull lawyerphx/rac
```

For cohort node

```shell
sudo docker run -dt --name="cohort" --network host lawyerphx/rac
sudo docker exec -i cohort ./bin/rac-server -node=co -preload -addr=$(Your cohort address)
```

For collaborator node

```shell
sudo docker run -dt --name="collaborator" --network host lawyerphx/rac
```



#### Test

A simple experiment program could be found in `experiment/experiment/py`

If you make some change to the code, make sure to rebuild the program:

```shell
make build
```

For local test, please set the `LocalTest` in `utils\utils.go` to True and run:

```shell
make local
```

This would add a 100ms delay to localhost for test.

