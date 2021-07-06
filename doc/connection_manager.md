# Connection Manager

ConnectionManager is a Class to store and maintain the connection at a provider between 2 requests on the same worker

## Why
In the **gunicorn** workflow, when we received a request, we create the connection, use it and destroy it
With this workflow, when we received a request, we ask a connection, create it if not exist, use it

The clean of connection runs in parallel and close after check if the connection is : 
- alive
- has been used since X times

Method __connect, __alive and __cancel are 

## How to use
````python
from toucan_connectors.connection_manager import ConnectionManager

snowflake_connection_manager = None
if not snowflake_connection_manager:
    snowflake_connection_manager = ConnectionManager(
        name='snowflake', timeout=10, wait=0.2, time_between_clean=10, time_keep_alive=600
    )

def __connect():
    return connection  

def __alive():
    return boolean

def __close():

def _get_connection(cm: ConnectionManager, identifier: str):
    connection = cm.get(
        identifier,
        connect_method=__connect,
        alive_method=__alive,
        close_method=__close,
    )
    return connection
````

# More information
To have more information about the process, you can refer at this [Confluence Documentation](https://toucantoco.atlassian.net/wiki/spaces/TTA/pages/3018653948/Connection+Manager+-+Query+pool?focusedCommentId=3021308042#comment-3021308042)