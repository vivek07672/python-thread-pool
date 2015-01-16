'''
Created on Jan 6, 2015

@author: vivek
'''
# consumer side coding to access the connection pool
# from database_connetion_pool import connection_provider
from database_connetion_pool import ConnectionProvider
from random import randint
from time import strftime
import csv
import datetime
import threading
import time
import traceback


"""
This Modules contains the methods and code to
test the MySQLdb pool implemented using blocking 
queue.
"""

CONNECTION_INFORMATION = [(None, None, None, None),
                          ("root", "", "127.0.0.1", "sakila"),
                          ("root", "", "127.0.0.1", "travel_ibibo"),
                          ("root", "", "127.0.0.1", "world"),
                          ]

CSV_FILE_LOCK = threading.Lock()

c = csv.writer(open("results.csv", "wb"))

c.writerow(["ThreadID    ","Enter time    ","after connection    ",
            "after query    ","after connection_close    ",
            "database    "])

class TestingThread(threading.Thread):
    """
    This class is used to simulate client which 
    wants to connect to database .
    Each thread is having a  different having 
    rando connection information .
    
    """
    def __init__(self,threadID):
        threading.Thread.__init__(self)
        random_number = randint(0, 3)
        self.__user = None
        self.set_user(CONNECTION_INFORMATION[random_number][0])
        self.__password = None
        self.set_password(CONNECTION_INFORMATION[random_number][1])
        self.__host = None
        self.set_host(CONNECTION_INFORMATION[random_number][2])
        self.__database = None
        self.set_database(CONNECTION_INFORMATION[random_number][3])
        self.__threadID = None
        self.set_thread_id(threadID)

    def get_thread_id(self):
        return self.__threadID

    def set_thread_id(self, value):
        self.__threadID = value

    def del_thread_id(self):
        del self.__threadID

    def get_user(self):
        return self.__user

    def get_password(self):
        return self.__password

    def get_host(self):
        return self.__host

    def get_database(self):
        return self.__database

    def set_user(self, value):
        self.__user = value

    def set_password(self, value):
        self.__password = value

    def set_host(self, value):
        self.__host = value

    def set_database(self, value):
        self.__database = value

    def del_user(self):
        del self.__user

    def del_password(self):
        del self.__password

    def del_host(self):
        del self.__host

    def del_database(self):
        del self.__database

    def run(self):
        connection_provider = ConnectionProvider()
        connection_provider.set_connection_info(user_name=self.get_user() ,
                                                host=self.get_host(), database=self.get_database(),
                                                password=self.get_password())
        try:
            start_time_enter = datetime.datetime.now().strftime("%H:%M:%S.%f")
            print "Thread ID %s name %s trying for connection %s" %(self.get_thread_id(),
                                                                  self.get_database(),
                                                                  strftime("%H:%M:%S"))
            mysql_connection = connection_provider.get_connection()
            time_after_getting_connection = datetime.datetime.now().strftime("%H:%M:%S.%f")
            print "Thread ID %s time %s got the connection " % (self.get_thread_id(), 
                                                                strftime("%H:%M:%S"))
            
            data = self.execute_query(mysql_connection)
            print "Thread ID %s released the connection to queue %s " % (self.get_thread_id(), 
                                                                strftime("%H:%M:%S"))
            time_after_query = datetime.datetime.now().strftime("%H:%M:%S.%f")
            connection_provider.close_connection(mysql_connection)
            time_after_connection_close = datetime.datetime.now().strftime("%H:%M:%S.%f")
            print("Thread-id = %s time = %s database = %s ---" % (self.get_thread_id(),
                                                                time.time() - start_time,
                                                                self.get_database() ))
            print "\n"
            print("\nThread-id = %s time = %f database = %s ---" % (self.get_thread_id(),
                                                                time.time() - start_time,
                                                                self.get_database() ))
            CSV_FILE_LOCK.acquire()
            database = None
            if self.get_database() is None:
                database = "new_database"
            else :
                database = self.get_database()
                
            c.writerow([
                        "    " + str(self.get_thread_id()),
                        "    " + start_time_enter,
                        "    " +  time_after_getting_connection,
                        "    " +  time_after_query,
                        "    " +  time_after_connection_close,
                        "    " +  database])
            CSV_FILE_LOCK.release()
        except:
            print "\n****Exception****"
            print traceback.format_exc()
            print("Thread-id = %s time = %s database = %s ---" % (self.get_thread_id(),
                                                                time.time() - start_time,
                                                                self.get_database() ))

    def execute_query(self, connection):
        start_time = time.time()
        cursor = connection.cursor()
        if self.get_database() == "new_database":
            cursor.execute("SELECT * from hotels_hoteldetails")
        elif self.get_database() == "sakila":
            cursor.execute("SELECT * from rental")
        elif self.get_database() == "travel_ibibo":
            cursor.execute("select * from common_station")
        elif self.get_database() == "employees":
            cursor.execute("SELECT * from dept_emp")
        elif self.get_database() == "world":
            cursor.execute("SELECT * from City")
        #print "thread id -- %s **** time taken -- %s" % (self.get_thread_id(),
        #                                                 time.time() - start_time)
        
threadList = []
start_time = time.time()
for threadCount in range(5):
    tempThread = TestingThread(threadCount)
    threadList.append(tempThread)
    tempThread.start()

for thread in threadList:
    thread.join()


print "--- %s seconds to execute thread  ---" % (time.time() - start_time)

"""
# making client request
print "connection 1"
connection_provider.set_connection_info()
cnx1 = connection_provider.get_connection()
print cnx1
print "connection 2"
cnx2 = connection_provider.get_connection()
print "connection 3"
cnx3 = connection_provider.get_connection()
print "connection 4"
cnx4 = connection_provider.get_connection()
print "connection 5"
cnx5 = connection_provider.get_connection()

print type(cnx5)
cursor = cnx5.cursor()
cursor.execute("SELECT VERSION()")
data = cursor.fetchone()
print "Database version : %s " % data
#connection_provider.set_connection_info()

#connection_provider.close_connection(cnx5)
print "connection 6"

cnx6 = connection_provider.get_connection()
print "done with waiting "
#disconnecting the database connections
#provider.clear_pool()
#deleting the provider ... leaving the refernce
#del provider
"""
