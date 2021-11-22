## Cohort

The cohort component for RAC protocol.
- It serves as a bridge between request from collaborator and local store.
- One `CohortStmt` with one `Shard` is needed for each node.

The unit tests for this component is used for development. If you want to run them, please change to local test setup

#### APIs

|          API          |                    Explanation                     | Thread-safe |
| :-------------------: | :------------------------------------------------: | :---------: |
|         Main          |  Start the cohort node with Args and config file.  |      Y      |
|     Break/Recover     | Used for local test to simulate the crash failure. |      Y      |
|        PreRead        | Handles a read only transaction and return values. |      T      |
| PreWrite/Commit/Abort |                  Common handlers                   |      T      |
|         Agree         |                    3PC handler                     |      T      |
|        Propose        |     For RAC propose phase, return the results      |      T      |


- Y for thread-safe, T for thread-safe between transactions.
