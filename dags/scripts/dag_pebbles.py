"""
@author: anilkdegala
"""
import os
import subprocess, signal
from collections import OrderedDict
from scripts.oracle_data_access import OracleDataAccess
from scripts.configurations import *
from scripts.oracle_log_file_parser import OracleLogFileParser

class DagPebbles:
    
    def __init__(self):
        print("DagPebbles::__init__()")
      
    def pipeline_enable_check(self, pipeline):
        oda = OracleDataAccess()
        return oda.pipeline_enable_check(pipeline)
    
    def save_pipeline_log(self, log_file):
        print("DagPebbles::save_pipeline_log() => log_file: {0}".format(log_file))
        olfp = OracleLogFileParser()
        results = olfp.get_log_file_dict_data(log_file)
        oda = OracleDataAccess()
        oda.save_pipeline_log(log_file, results)       
        
    def validate_pipeline_log(self, log_file):
        print("DagPebbles::validate_pipeline_log()") 
        #TODO::
        return True
        
     
 
    