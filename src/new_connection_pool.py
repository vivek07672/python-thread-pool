'''
Created on Jan 13, 2015

@author: vivek
'''
import ConfigParser
import MySQLdb
import threading
import time
import traceback

RLOCK = threading.RLock()
COND_VAR_SYNC = threading.Condition(RLOCK)


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


class ConnectionList(object):
    def __init__(self):
        object.__init__(self)
        self.list = []
        self.last_accesstime =0
        self.connection_count =0

    def get_list_element(self):
        if self.get_list_length():
            connection = self.list[0]
            del self.list[0]
            return connection

    def get_list_length(self):
            return len(self.list)

    def get_last_accesstime(self):
            return self.last_accesstime

    def add_connection(self,cnx):
            self.list.append(cnx)

    def inc_connection_count(self):
            self.connection_count = + 1

    def dec_connection_count(self):
            self.connection_count = - 1


class NewConnectionPool(object):
    def __init__(self):
        object.__init__(self)
        self.configurationParser = None
        self.set_configuration_parser()
        self.pool_dict = {}
        self.lock_count = {}
        self.total_connection = 0
        self.default_user = self.configurationParser.\
                    get("Configuration Data", "userName")
        self.default_host = self.configurationParser.\
                    get("Configuration Data", "host")
        self.default_password = self.configurationParser.\
                    get("Configuration Data", "password")
        self.default_database = self.configurationParser.\
                    get("Configuration Data", "database")
        self.pool_size = self.configurationParser.\
                    get("Configuration Data", "poolSize")
        if int(self.pool_size) is None:
            raise"pool size can not be zero "

    def set_configuration_parser(self):
        self.configurationParser = ConfigParser.ConfigParser()
        path = "/home/vivek/Documents/workspace/ConnectionPool/src/configuration.txt"
        self.configurationParser.readfp(open(path))

    def get_connection(self, user=None, host=None,
                       database=None, password=None,threadId = None):
        COND_VAR_SYNC.acquire()
        self.lock_count[threadId] = 1
        if (user is None) or (host is None) or\
            (database is None) or (password is None):  # take data from parser
                user = self.default_user
                password = self.default_password
                host = self.default_host
                database = self.default_database
        #print "thread connection " + str(self.total_connection)
        '''if self.total_connection == int(self.pool_size):
            print "Thread is %s is waiting " %threadId
            try:
                COND_VAR_SYNC.wait(1)                #self.get_connection(user, host, database, password, threadId)
            except:
                print "\n****ConnectionException****" + traceback.format_exc()
                raise "too many connection"'''
        connection_info = ConnectionInfo(user, host, database, password)
        if connection_info in self.pool_dict:  # connection list is there
            connection_list = self.pool_dict[connection_info]
            if connection_list.get_list_length():
                connection = connection_list.get_list_element()
                connection_list.last_accesstime = time.time()
                self.total_connection = +1
                if self.total_connection < int(self.pool_size):
                    COND_VAR_SYNC.notify()
                    COND_VAR_SYNC.release()
                    self.lock_count[threadId] = self.lock_count[threadId] - 1
                return connection
            else:
                connection_list.inc_connection_count()
                connection_list.last_accesstime = time.time()
                self.total_connection = self.total_connection + 1
                if self.total_connection < int(self.pool_size):
                    COND_VAR_SYNC.notify()
                    COND_VAR_SYNC.release()
                    self.lock_count[threadId] = self.lock_count[threadId] - 1
                return MySQLdb.Connect(host, user, password, database)
        else:
            self.pool_dict[connection_info] = ConnectionList()
            self.pool_dict[connection_info].inc_connection_count()
            self.total_connection = self.total_connection + 1
            self.pool_dict[connection_info].last_accesstime = time.time()
            if self.total_connection < int(self.pool_size):
                    COND_VAR_SYNC.notify()
                    COND_VAR_SYNC.release()
                    self.lock_count[threadId] = self.lock_count[threadId] - 1
            return MySQLdb.Connect(host, user, password, database)

    def put_connection(self, user, host,
                       database, password, cnx, threadId):
        COND_VAR_SYNC.acquire()
        self.lock_count[threadId] = self.lock_count[threadId] + 1
        if (user is None) or (host is None) or\
            (database is None) or (password is None):  # take data from parser
                user = self.default_user
                password = self.default_password
                host = self.default_host
                database = self.default_database
        connection_info = ConnectionInfo(user, host, database, password)

        connection_list = self.pool_dict[connection_info]
        connection_list.list.append(cnx)
        # connection_list.list.last_accesstime = time.time()
        self.total_connection = self.total_connection - 1
        #print "@@@##&&&thread lock count " + str(self.lock_count[threadId])
        if self.lock_count[threadId] == 2:
            COND_VAR_SYNC.notify()
            COND_VAR_SYNC.release()
            COND_VAR_SYNC.release()
        else:
            COND_VAR_SYNC.notify()
            COND_VAR_SYNC.release()
