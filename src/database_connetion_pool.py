'''
Created on Jan 6, 2015

@author: vivek
'''

from mysql import connector
from Queue import Queue, Empty
from compiler.ast import TryExcept
from symbol import try_stmt
import ConfigParser
import MySQLdb
import threading
import time

MAX_CONNECTION_POOL_SIZE = 32

cond_var = threading.Condition()

class ConnectionProvider(object):
    connection_pool_dict = {}

    def __init__(self):
        #for backup ,user_name = None ,password = None,
        #database =None host = "127.0.0.1"
        self.configurationParser = None
        self.set_configuration_parser()
        self._user_name = None
        #self.set_user_name(user_name)
        self._password = None
        #self.set_password(password)
        self._host = None
        #self.set_host(host)
        self._database = None
        #self.set_database()
        self._pool_size = None

    def set_configuration_parser(self):
        self.configurationParser = ConfigParser.ConfigParser()
        path = "/home/vivek/Documents/workspace/ConnectionPool/src/configuration.txt"
        self.configurationParser.readfp(open(path))

    def _set_pool_size(self):
        """
        This method sets the pool size from the configuration file .
        If pool size if less than 0 or greater than maximum possible
        size (database_connection_pool.MAX_CONNECTION_POOL_SIZE),then
        raises Atribute error.
        """
        self._pool_size = int(self.configurationParser.get("Configuration Data", "poolSize"))
        if self._pool_size < 0 or self._pool_size > MAX_CONNECTION_POOL_SIZE :
            raise AttributeError("Size of connection pool should be"
                                 "greater than 0 or less than 32")

    def set_username(self, user_name):
        """
        This methods set the user name for the MySQL connection
        Raises an Attribute error if user name is None
        """
        if user_name is None:
            self._user_name = self.configurationParser.get("Configuration Data", "userName")
        else:
            self._user_name = user_name
        if self._user_name is None:
            raise AttributeError("user_name can not be '{0}' ".format(self._user_name))

    def set_password(self, password):
        """This method sets the password for database connection."""
        if password is None:
            self._password = self.configurationParser.get("Configuration Data", "password")
        else:
            self._password = password

    def set_host(self, host):
        """
        This method set the host name for the MySQL connection
        Raises an Attribute error if host name is None
        """
        if host is None:
            self._host = self.configurationParser.get("Configuration Data", "host")
        else:
            self._host = host
        if self._user_name is None:
            raise AttributeError("host can not be '{0}' ".format(self._host))


    def set_database(self, database):
        """
        This method set the database name for the MySQL connection
        Raises an Attribute error if host name is None
        """
        if database is None:
            self._database = self.configurationParser.get("Configuration Data", "database")
        else:
            self._database = database
        if self._database is None:
            raise AttributeError("database can not be '{0}' ".format(self._database))


    def set_connection_info(self, user_name=None, host=None,
                            database=None, password=None):
        """
        This method sets the conenctioninfo in the connection provider
        This method must be called by the client to set the 
        provider attributes.
        """
        self.set_password(password)
        #print self._password
        self.set_username(user_name)
        #print self._user_name
        self.set_host(host)
        #print self._host
        self.set_database(database)
        #print self._database
        self._set_pool_size()
        #print self._pool_size

    def get_connection(self):
        """
        This method gets a connection from the connection pool 
        present in dictionary .
        It takes the connection from pool if connection 
        pool is there in the internal dictionary.
        """
        cond_var.acquire()
        conection_credentials = ConnectionInfo(self._user_name,
                                               self._host, self._database,
                                               self._password)
            # print("---  ThreadPool size %s---" % ConnectionProvider.\
            #      connection_pool_dict.__len__())
        if conection_credentials in ConnectionProvider.connection_pool_dict:
           connection_pool = ConnectionProvider.\
                connection_pool_dict[conection_credentials]
           if connection_pool.get_pool_size() == 0:
               cond_var.wait(5)
           pool_connection = connection_pool.get_connection()
        else:
            # print "in the else block "
            ConnectionProvider.connection_pool_dict[conection_credentials] = \
            ConnectionPool(self._pool_size, self._user_name,
                               self._host, self._password, self._database)
            connection_pool = ConnectionProvider.\
            connection_pool_dict[conection_credentials]
            pool_connection = connection_pool.get_connection()
        cond_var.notify()
        cond_var.release()
        return pool_connection

    def close_connection(self, connection):
        cond_var.acquire()
        conection_credentials = ConnectionInfo(self._user_name,
                                               self._host,
                                               self._database, self._password)
        connection_pool = ConnectionProvider.\
        connection_pool_dict[conection_credentials]
        connection_pool.add_connection(connection)
        cond_var.notify()
        cond_var.release()


class ConnectionInfo(object):
    def __init__(self, user_name, host,
                 database, password):
        self.user_name = user_name
        self.host = host
        self.database = database
        self.password = password

    def __hash__(self):
        return hash((self.user_name, self.host,
                    self.database, self.password))

    def __eq__(self, other):
        return (self.user_name, self.host,
                    self.database, self.password) == \
                    (other.user_name, other.host,
                     other.database, other.password)


class ConnectionPool(object):
    def __init__(self, pool_size, user_name, host, password, database):
        self._pool_size = pool_size
        self.connection_queue = Queue(self._pool_size)
        connection_count = 0
        #print "poolSize is  = " + str(pool_size)
        while (connection_count < int(pool_size)):
            self.connection_queue.put(MySQLdb.Connect(host, user_name,
                                                  password, database))
            connection_count = connection_count + 1
        #print self.connection_queue.qsize()

    def add_connection(self, connection):
        """This methods adds mysql connection
        in the queue . Then queue acts as a pool of connection
        for furthur connection requests """
        # add the connection in the queue
        self.connection_queue.put(connection, False)

    def get_connection(self):
        """
        This method gives a connection present
        in the queue .Since queue is acting as a blocking queue ,
        so the request gets blocked until it get
        one connection from the queue
        """
        """if self.connection_queue.qsize() == 0 and try == 0:
            raise "there are too many connections plz try again"
        if self.connection_queue.qsize() == 0:
            time.sleep(5)
            try = try - 1
            self.get_,connection(try)"""
        try:
            return self.connection_queue.get(False)
        except Empty:
            raise "Too many connections ... PLZ try again"

    def close_connection(self, connection):
        """This method does not close
        the connection ,but keeps the
        connection instance back in the queue.
        """
        self.connection_queue.put(connection)

    def get_pool_size(self):
        """This method returns the total numbers
        of the connection present in queue.
        """
        self.connection_queue.qsize()

    def clear(self):
        """This method disconnects all the
        MySQL connections present in the queue ."""
        while self.connection_queue.qsize():
            conn = self.connection_queue.get_nowait()
            conn.disconnect()


