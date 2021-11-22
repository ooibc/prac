#### APIs

|       API        |                      Explanation                       | Thread-safe |
| :--------------: | :----------------------------------------------------: | :---------: |
|       Main       | Start the collaborator node with Args and config file. |      Y      |
| NewDBTransaction |     Create an object to handle a job from clients.     |      T      |
|     PreRead      |     PreRead the data with a read only transaction.     |      T      |
|    SubmitTxn     |   A common handler for three protocols. (write only)   |      T      |

- Y for thread-safe, T for thread-safe between transactions.


