"""
@author: anilkdegala
"""
import os
from collections import OrderedDict
import json
from scripts.configurations import *

class OracleLogFileParser:
      
    def __init__(self):
        print("OracleLogFileParser::__init__()")
        self.exported_object_list = []
        self.dump_file_list = []
        self.exported_objects = []
        self.dmp_files = []
        
    def parse_log_file(self, file = "no_file_provided.log"): 
        dump_file_list = []
        with open(DATA_LOCATION + "/" + file) as fp:
            lines = fp.readlines()
            for line in lines: 
                line = line.strip()
                if line.startswith('. . exported '):
                    self.exported_object_list.append(line)
                elif line.startswith('/rc_exp/data_dump/'): 
                    self.dump_file_list.append(line)
        
    def get_schema(self, str): 
        str = str.replace("\"","")
        str = str.split(".")[0] 
        return str

    def get_table(self, str):
        str = str.replace("\"","")
        str = str.split(".")[1]
        if str.find(":")>-1: 
            str = str.split(":")[0] 
            
        return str
        
    def get_node(self, str):
        str = str.replace("\"","")
        str = str.split(".")[1]  
        if str.find(":") >-1:  
            str = str.split(":")[1]
        else: 
            str = None 
            
        return str
        
    def get_size(self, str):
        return float(str.strip()) 
        
    def get_type(self, str):
        return str.strip() 
        
    def get_rows(self, str):
        return int(str.strip())                                             
 
    def build_exported_object_list(self):  
        
        for line in self.exported_object_list:
            x = line.split()
            #print(x[3] ,x[4], x[5], x[6])
            
            exp_obj_dict = {
                "schema": self.get_schema(x[3]),
                "table": self.get_table(x[3]),
                "node": self.get_node(x[3]),
                "size": self.get_size(x[4]),
                "type": self.get_type(x[5]),
                "rows": self.get_rows(x[6]),
            }
            
            self.exported_objects.append(exp_obj_dict)
            
        
        
    def get_log_object(self):
        return {
            "exported_objects": self.exported_objects,
            "dmp_files": self.dmp_files
        }
    
        
    def build_dmp_file_list(self): 
        for line in self.dump_file_list:
            x = line.split("/")
            self.dmp_files.append(x[4])
            
    def print_log_json(self): 
        return json.dumps(self.get_log_object(), indent = 4)   
    
    def get_log_json(self): 
        return json.dumps(self.get_log_object(), indent = 4)     
    
    def get_log_file_json_data(self, file):
        self.parse_log_file(file)
        self.build_exported_object_list()
        self.build_dmp_file_list() 
        return self.get_log_json()
                    
    def get_log_file_dict_data(self, file):
        self.parse_log_file(file)
        self.build_exported_object_list()
        self.build_dmp_file_list() 
        return self.get_log_object()
     
if __name__ == '__main__':
    olfp = OracleLogFileParser()
    olfp.parse_log_file()
    olfp.build_exported_object_list()
    olfp.build_dmp_file_list() 
    print(olfp.print_log_json()) 