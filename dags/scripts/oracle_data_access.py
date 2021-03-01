"""
@author: anilkdegala
"""
import cx_Oracle
import os
from collections import OrderedDict
import traceback
from scripts.pipeline_utils import PipelineUtils


class OracleDataAccess:
    
   def get_db_connection(self):
        connection_string = os.environ.get(
            "ORACLE_CONNECTION_STRING", ""
        )
        con = None
        try:
            con = cx_Oracle.connect(connection_string)
        except Exception as e:
            print(e)

        return con
    
   def pipeline_enable_check(self, pipeline):
        print("pipeline_enable_check() for {0}: ".format(pipeline))
        retValue = False
        
        conn = None
        cur = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor() 
            statement = (
                "select pipeline, enabled from  PIPELINE_CONTROL_DATA where pipeline in (:pipeline)"
            )
            cur.execute(statement, {"pipeline": pipeline})
            row = cur.fetchone() 
    
            if row == None:
                retValue = False
            elif row[1] == 'Y':
                retValue = True
                
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()

        return retValue    
    
    
    
   def get_current_pipeline_dmp_files(self):
        print("get_current_pipeline_dmp_files")    
        conn = None
        cur = None
        files = []
        try:
            conn = self.get_db_connection()
            cur = conn.cursor() 
            statement = (
                "select log_file_id from  PIPELINES where status in ('RUNNING')"
            )
            cur.execute(statement, {})
            row = cur.fetchone() 
            
            print(row[0])
            log_file_id = row[0]
            files = self.get_files_to_download(log_file_id)
            
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()

        return files
        
     
   def get_files_to_download(self, log_file_id):
        print("get_files_to_download() for {0}: ".format(log_file_id))
        files = []
        
        conn = None
        cur = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor() 
            statement = (
                "select log_file_id, dmp_file_name from  LOG_FILE_DMP_FILES where log_file_id in (:log_file_id)"
            )
            cur.execute(statement, {"log_file_id": log_file_id})
            rows = cur.fetchall() 
            
            print(rows)
            
            if rows != None:
                for row in rows:
                    files.append(row[1])
                    
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()

        return files
    
   def save_pipeline_log(self, log_file_name, log_data):
        print("save_pipeline_log(): log_file_name:{0}".format(log_file_name))
        
        conn = None
        cur = None
        try:
             
            conn = self.get_db_connection()
            cur = conn.cursor() 
            pu = PipelineUtils()
            statement = 'insert into PIPELINE_LOG_FILE(id, log_file_name) values (:id, :log_file_name)'
            cur.execute(statement, (pu.generate_uuid(), log_file_name))
             
             
            statement = (
                "select id,log_file_name from  PIPELINE_LOG_FILE where log_file_name in (:log_file_name)"
            )
            cur.execute(statement, {"log_file_name": log_file_name})
            row = cur.fetchone() 
            log_file_id = None
            
            if row != None:
                log_file_id = row[0] 
            
            for key,value in log_data.items():  
                if key == 'dmp_files':
                    for v in value:
                        statement = 'insert into LOG_FILE_DMP_FILES(log_file_id, dmp_file_name) values (:log_file_id, :dmp_file_name)'
                        cur.execute(statement, (log_file_id, v)) 
                       
                if key == 'exported_objects':
                    for v in value:     
                        statement = 'insert into LOG_FILE_EXPORT_OBJECTS(log_file_id, schema_name, table_name, node, file_size, file_size_type, rows_count) values (:log_file_id, :schema_name, :table_name, :node, :file_size, :file_size_type, :rows_count)'
                        cur.execute(statement, (log_file_id, v['schema'],v['table'],v['node'],v['size'],v['type'],v['rows']))
                                      
            
            
            statement = 'insert into PIPELINES(log_file_id, status) values (:log_file_id, :status)'
            cur.execute(statement, (log_file_id, 'RUNNING')) 
                         
            conn.commit()     
            
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()  