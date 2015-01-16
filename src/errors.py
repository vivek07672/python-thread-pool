'''
Created on Jan 8, 2015

@author: vivek
'''

class Error(StandardError):
    def __init__(self,msg= None ,error_no = None):
        super(self).__init__();
        