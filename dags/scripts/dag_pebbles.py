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
        
    def get_files(self, **kwargs):
        print("DagPebbles::get_files() for {0}".format(kwargs['type']))
        oda = OracleDataAccess() 
        files = oda.get_files(**kwargs)
        print(files)
        return files       
    
    def get_current_pipeline(self):
        print("DagPebbles::get_current_pipeline()")
        oda = OracleDataAccess()
        return oda.get_current_pipeline()
    
    
    def stage_dmp_files(self, **kwargs):
        print("DagPebbles::stage_dmp_files()")
        oda = OracleDataAccess()
        oda.stage_dmp_files(**kwargs)
        
        
      
     
 
    