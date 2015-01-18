'''
Created on Jan 14, 2015
This class is a creates MySQL Connection
pool by the use of hashmaps .

Pool uses Conditional Lock to synchronise threads.
Thread waiting have a time out so there is never a
condition of dead lock.

Working threads may get the errors Like -

ConnectionError - there is no passive connection in
                  pool.
PoolSizeError - if the size of pool is 0.

@author: vivek
'''
import ConfigParser
import MySQLdb
import threading

# RLOCK = threading.RLock()
COND_VAR_SYNC = threading.Condition()


class Connection(object):
    """This class is the acting as the MySQl connection
    wrapper ,which is holding the connection and its
    related information ."""
    def __init__(self, user_name, host,
                 database, password, connection_number):
        """

        Create a connection to the database. It is strongly recommended
        that you only use keyword parameters.

        host
          string, host to connect

        user_name
          string, user to connect as

        password
          string, password to use

        database
          string, database to use

        connection_number
          string, pool connection numbers
        """
        self.user_name = user_name
        self.host = host
        self.database = database
        self.password = password
        self.connection_number = connection_number
        try:
            self.cnx = MySQLdb.Connect(host, user_name, password, database)
        except:
            pass

    def __hash__(self):
        return hash((self.user_name, self.host,
                    self.database, self.password))

    def __eq__(self, other):
        return (self.user_name, self.host,
                self.database, self.password) == \
            (other.user_name, other.host,
                other.database, other.password)


class ConnectionPool(object):
    """Class to make a Connection Pool
    for multi-threaded application
    """

    def __init__(self):
        """
        Creates a Pool of connection for MySQL database.
        The class reads user_name , host ,password ,
        database and pool_size as default from the
        supplied configuration file .
        """
        object.__init__(self)
        self.list = []  # to contain connections
        self.database_dict = {"passive": []}
        self.connection_dict = {}
        self.connection_created = 0
        self.configurationParser = None
        self.set_configuration_parser()
        self.default_user = self.configurationParser.\
            get("Configuration Data", "userName")
        self.default_host = self.configurationParser.\
            get("Configuration Data", "host")
        self.default_password = self.configurationParser.\
            get("Configuration Data", "password")
        self.default_database = self.configurationParser.\
            get("Configuration Data", "database")
        self.pool_size = int(self.configurationParser.
                             get("Configuration Data", "poolSize"))
        if self.pool_size is None:
            raise PoolSizeError("pool size can not be zero ")

    def set_configuration_parser(self):
        """
        Sets the config parser to point a file in
        storage . This parser is used to fetch the
        default values
        """
        self.configurationParser = ConfigParser.ConfigParser()
        path = "/home/vivek/Documents/workspace/ConnectionPool/src/configuration.txt"
        self.configurationParser.readfp(open(path))

    def get_connection(self, user=None, host=None,
                       database=None, password=None):

        """
        This method returns a connection from the pool
        """

        if (user is None) or (host is None) or\
           (database is None) or (password is None):
                user = self.default_user
                password = self.default_password
                host = self.default_host
                database = self.default_database
        COND_VAR_SYNC.acquire()
        if self.connection_created < self.pool_size:
            connection = None
            if database in self.database_dict.keys():
                cnx_list = self.database_dict[database]
                if len(cnx_list):
                    connection = self.get_passive_connection(cnx_list)
                    COND_VAR_SYNC.release()
                    return connection
                else:
                    self.connection_created = self.connection_created + 1
                    connection = Connection(user, host, database, password,
                                            self.connection_created)
                    COND_VAR_SYNC.release()
                    return connection
            else:
                self.database_dict[database] = []
                self.connection_created = self.connection_created + 1
                connection = Connection(user, host, database, password,
                                        self.connection_created)
                COND_VAR_SYNC.release()
                return connection
        elif self.connection_created == self.pool_size:
            if database in self.database_dict.keys():
                cnx_list = self.database_dict[database]
                if len(cnx_list):
                    connection = self.get_passive_connection(cnx_list)
                    COND_VAR_SYNC.release()
                    return connection
                else:
                    if len(self.database_dict["passive"]):
                        connection = self.close_passive(user, host,
                                                        database, password)
                        COND_VAR_SYNC.release()
                        return connection
                    else:
                        connection = self.wait_for_connection(user, host,
                                                              database,
                                                              password)
                        return connection
            else:
                if len(self.database_dict["passive"]):
                    self.database_dict[database] = []
                    new_connection = self.close_passive(user, host,
                                                        database, password)
                    COND_VAR_SYNC.release()
                    return new_connection
                else:
                    connection = self.wait_for_connection(user, host,
                                                          database, password)
                    return connection

    def wait_for_connection(self, user, host, database, password):

        """
        This method keeps the thread waiting for 60*1(attempts * 1 sec)
        And as the thread get the connection free by notification
        or time out , it takes the connections from pool and leaves the
        conditional lock.

        host
          string, host to connect

        user
          string, user to connect as

        password
          string, password to use

        database
          string, database to use
        """

        attempt = 60
        connection = None
        while not len(self.database_dict["passive"]):
            COND_VAR_SYNC.wait(1)
            if attempt == 0 and not len(self.database_dict["passive"]):
                COND_VAR_SYNC.release()
                raise ConnectionError("too many connection")
            attempt = attempt - 1
        if database in self.database_dict.keys():
            cnx_list = self.database_dict[database]
            if len(cnx_list):
                connection = self.get_passive_connection(cnx_list)
            else:
                if len(self.database_dict["passive"]):
                    connection = self.close_passive(user, host,
                                                    database, password)
        else:
            self.database_dict[database] = []
            connection = self.close_passive(user, host, database, password)
        COND_VAR_SYNC.release()
        return connection

    def get_passive_connection(self, cnx_list):

        """
        This method returns a passive connection from connection pool
        cnx_list
          list, having passive connection key
        """

        connection_key = cnx_list.pop(0)
        self.database_dict["passive"].remove(connection_key)
        connection = self.connection_dict.pop(connection_key)
        return connection

    def close_passive(self, user, host, database, password):

        """This method takes deletes a passive connection
        and returns the new connection with the given function
        parameters

        host
          string, host to connect

        user
          string, user to connect as

        password
          string, password to use

        database
          string, database to use
        """

        connection_key = self.database_dict["passive"].pop(0)
        connection = self.connection_dict.pop(connection_key)
        self.database_dict[connection.database].remove(connection_key)
        new_connection = Connection(user, host, database,
                                    password, connection.connection_number)
        del connection
        return new_connection

    def put_connection(self, connection):

        """
        This method puts the connection back in the
        pool for further query .
        connection
          connection to be listed as passive
        """

        COND_VAR_SYNC.acquire()
        # print "relesing ...:)"
        cnx_list = self.database_dict[connection.database]
        cnx_list.append(connection.connection_number)
        self.database_dict["passive"].append(connection.connection_number)
        self.connection_dict[connection.connection_number] = connection
        COND_VAR_SYNC.notify()
        COND_VAR_SYNC.release()

    def clear_pool(self):

        """
        This method closes all the connection
        in the pool at operation stop event 
        """

        for connection in self.list:
            del connection


class Error(Exception):

    """
    Base class for error in connection
    pool operation .
    """

    def __init__(self, message):
        self.message = message

    def get_message(self):
        return self.message


class ConnectionError(Error):

    """
    Error class used to raise connection
    not found error
    """

    def __int__(self, message):
        self.message = message


class PoolSizeError(Error):

    """
    Error class used to raise error
    when pool size is equal to 0
    """

    def __int__(self, message):
        self.message = message


class MySQLConfigError(Error):
    def __int__(self, message):
        self.message = message
