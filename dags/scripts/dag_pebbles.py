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
        
        
    def get_files_to_download(self, log_file_id):
        print("DagPebbles::get_files_to_download() => log_file_id: {0}".format(log_file_id))
        oda = OracleDataAccess()
        if log_file_id == None:
            files = oda.get_current_pipeline_dmp_files()
        else:
            files = oda.get_files_to_download(log_file_id)  
            
        return files         
       
     
    def get_files_to_decrypt(self, log_file_id):
        print("DagPebbles::get_files_to_decrypt() => log_file_id: {0}".format(log_file_id))
        oda = OracleDataAccess()
        if log_file_id == None:
            files = oda.get_current_pipeline_dmp_files()
        else:
            files = oda.get_files_to_download(log_file_id)  
            
        return files    
    
    def get_files_to_transfer(self, log_file_id):
        print("DagPebbles::get_files_to_transfer() => log_file_id: {0}".format(log_file_id))
        oda = OracleDataAccess()
        if log_file_id == None:
            files = oda.get_current_pipeline_dmp_files()
        else:
            files = oda.get_files_to_download(log_file_id)  
            
        return files    
      
     
 
    