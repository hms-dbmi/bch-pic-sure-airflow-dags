"""
@author: anilkdegala
"""
import os
import subprocess, signal
from collections import OrderedDict
from scripts.oracle_data_access import OracleDataAccess
from scripts.configurations import *
from scripts.oracle_log_file_parser import OracleLogFileParser
import csv

class DagPebbles:
    
    def __init__(self):
        print("DagPebbles::__init__()")
      
    def pipeline_enable_check(self, pipeline):
        oda = OracleDataAccess()
        return oda.pipeline_enable_check(pipeline)
    
    def save_pipeline_log(self, log_file_path, log_file):
        print("DagPebbles::save_pipeline_log() => log_file: {0}".format(log_file))
        olfp = OracleLogFileParser()
        results = olfp.get_log_file_dict_data(log_file)
        oda = OracleDataAccess()
        oda.save_pipeline_log(log_file_path, log_file, results)       
        
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
        
    def stage_dmp_files1(self, **kwargs):
        print("DagPebbles::stage_dmp_files1()")
        oda = OracleDataAccess()
        oda.stage_dmp_files1(**kwargs)
        
    def stage_dmp_files2(self, **kwargs):
        print("DagPebbles::stage_dmp_files2()")
        oda = OracleDataAccess()
        oda.stage_dmp_files2(**kwargs)  
        
    def stage_custom_dmp_files(self, **kwargs):
        print("DagPebbles::stage_custom_dmp_files()")
        oda = OracleDataAccess()
        oda.stage_custom_dmp_files(**kwargs)    
        
    def load_data(self, **kwargs):
        print("DagPebbles::load_data()")
        oda = OracleDataAccess()
        oda.load_data(**kwargs)        
        
        
    def get_download_key(self, s3_bucket, folder_path, s3_file):
        s3_bucket = s3_bucket.strip() if s3_bucket!= None  else ""
        folder_path = folder_path.strip() if folder_path!= None  else ""
        s3_file = s3_file.strip() if s3_file!= None  else ""
        
        download_key = s3_bucket
     
        if(folder_path!=None and len(folder_path) >0):
            download_key = download_key + "/"+folder_path
            
        if(s3_file!=None and len(s3_file) >0):
            download_key = download_key + "/"+s3_file
            
        print("get_download_key: download_key: ",download_key)
    
        return download_key
    
    
    def recreate_bch_hpds_data(self): 
        oda = OracleDataAccess()
        data = oda.recreate_bch_hpds_data() 
        
    def save_hpds_package_file_name(self, packed_file_name):
        oda = OracleDataAccess()
        data = oda.save_hpds_package_file_name(packed_file_name) 
        
    def get_hpds_packed_file_name(self):
        oda = OracleDataAccess()
        return oda.get_hpds_packed_file_name()
    
    def save_pipeline_state(self, **kwargs):
        oda = OracleDataAccess()
        return oda.save_pipeline_state(**kwargs)   
    
    def stage_biobank_file(self, **kwargs):
        print("DagPebbles::stage_biobank_file()")
        oda = OracleDataAccess()
        # TEMP: p00000159_biobank.csv
        biobank_file = 'p00000159_biobank.csv'
        bad_data = [] 
        biobank_data = []
        with open(DATA_LOCATION + "/" + biobank_file, 'r') as file:
            reader = csv.reader(file,delimiter=',')
            next(reader)
            cnt = 0
            for row in reader: 
                cnt = cnt + 1
                try:
                    #print(type(row), len(row))
                    row_data = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],row[28],row[29],row[30],row[31],row[32],row[33],row[34],row[35],row[36],row[37],row[38],row[39],row[40],row[41],row[42],row[43],row[44],row[45],row[46],row[47],row[48],row[49],row[50],row[51],row[52],row[53])
                    biobank_data.append(row_data) 
                except Exception as e:
                    print(row)
                    print(e)
                    bad_row = (row,) 
                    bad_data.append(bad_row)
                    
            
        
        oda.stage_biobank_file(biobank_data)
        
        
    def stage_mapping_file(self, **kwargs):
        print("DagPebbles::stage_mapping_file()")
        oda = OracleDataAccess()
        oda.stage_mapping_file(**kwargs) 
        
    def stage_uuid_mapping_file(self, mapping_file):
        print("DagPebbles::stage_uuid_mapping_file()")
        
        oda = OracleDataAccess() 
        bad_data = [] 
        mappings_data = []
        with open(DATA_LOCATION + "/" + mapping_file, 'r') as file:
            reader = csv.reader(file,delimiter=',')
            next(reader)
            cnt = 0
            for row in reader: 
                cnt = cnt + 1
                try:
                    mapping = (int(row[0].strip()), int(row[1].strip()),row[2].strip() , row[3].strip()) 
                    mappings_data.append(mapping) 
                except IndexError as e1:
                    print(row)
                    print(e1)
                    bad_row = (row[0],"","","") 
                    bad_data.append(bad_row)
                    pass
                except Exception as e:
                    print(row)
                    print(e)
                    bad_row = (row[0], row[1],row[2], row[3]) 
                    bad_data.append(bad_row)
                    pass
                    #raise e
        print("Mapping Data Counts")
        print("Total records: ",cnt)
        print("Good records: ",len(mappings_data))
        print("Bad records: ",len(bad_data))
        
        
        print(bad_data)
        oda.stage_uuid_mapping_file(mappings_data)
        oda.stage_mapping_data_bad_rows(bad_data) 
                         
        
    def clean_hpds_source_data(self):
        print("DagPebbles::clean_hpds_source_data()")
        oda = OracleDataAccess()
        oda.clean_hpds_source_data()     
        
    def post_stage_redefinition(self):
        print("DagPebbles::post_stage_redefinition()")
        oda = OracleDataAccess()
        oda.post_stage_redefinition()              
        

             