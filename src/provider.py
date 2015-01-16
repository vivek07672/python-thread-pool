'''
Created on Jan 6, 2015

@author: vivek
'''
import re

class connectionProvider(object):

    def __init__(self):
        #self.pool = ConnectionPool()
        pass

    def get_connection(self):
        """This method gets a connection from the pool"""
        return self.pool.getConnection()

    def close_connection(self, connection):
        """This method returns the connection 
            to the pool furthur request """
        self.pool.closeConnection(connection)

    def clear_pool(self):
        """This methods disconnects all the 
        connection in the pool """
        self.pool.clear()



class A(object):
    static1 = 13
    static2 = 14
    def __init__(self):
        self.value = 10
        self._private =20
        self.__superPrivate = 30
    def get_value(self):
            return self.value
        
    def ret_private(self):
            return self._private
        
    def ret__superPrivate(self):
            print "hihii"
            return self.__superPrivate

class B(A):
    static1 = 17
    static3 = 19
    def __init__(self):
        A.__init__(self)
        self.value = 75
        self._private =283430
        self.__superPrivate = 12310
 
        
a = A()
print a.value
print a._private
print a._A__superPrivate 
print a.get_value()
print a.ret_private()
print a.static1
print a.static2
print a.ret__superPrivate()


print "now b"
b= B()
print b.__dict__
print b.value
print b._private
print b._A__superPrivate
print b._B__superPrivate
print b.static1
print b.static2
print b.static3
print "now functions "
print b.get_value()
print b.ret_private()
print b.ret__superPrivate()

re.match( r'(.*) are (.*?) .*', "I am a good boy ", re.M|re.I)
        