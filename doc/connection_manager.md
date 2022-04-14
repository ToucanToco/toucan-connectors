# Connection Manager

ConnectionManager is a Class to store and maintain connections to a data provider, outliving the request response cycle at a single worker level.

## Why
In the previous workflow, when we received a live data request, we created a connection, used it once and immediately closed it.
With this workflow, when we received a request, we ask a connection, create it if not exist, use it but then it is kept live for further requests.

There is a thread that cleans ConnectionManager and closes connections after checking  first :
- whether it is alive
- if has been used since X times

Method __connect, __alive and __cancel are mandatory to ensure proper functioning

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
For have more information about the process, you can refer at this [Confluence Documentation](https://toucantoco.atlassian.net/wiki/spaces/TTA/pages/3018653948/Connection+Manager+-+Query+pool?focusedCommentId=3021308042#comment-3021308042)
