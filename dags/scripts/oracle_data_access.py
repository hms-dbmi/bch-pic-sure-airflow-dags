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
    
   def get_current_pipeline(self):
        print("get_current_pipeline")    
        conn = None
        cur = None
        pipeline = OrderedDict()
        files = []
        try:
            conn = self.get_db_connection()
            cur = conn.cursor() 
            statement = (
                "select a.id, a.log_file_name,a.created_at, b.status from PIPELINE_LOG_FILE a, PIPELINES b  where b.status = 'RUNNING' and a.id = b.log_file_id"
            )
            cur.execute(statement, {})
            row = cur.fetchone() 
            
            pipeline = {
                "id": row[0],
                "log_file_name": row[1],
                "created_at": row[2],
                "status": row[3],
                "files": files
            }
            
            id = row[0]
            
            statement = (
                "select log_file_id, dmp_file_name from  LOG_FILE_DMP_FILES where log_file_id in (:log_file_id)"
            )
            cur.execute(statement, {"log_file_id": id})
            rows = cur.fetchall() 
            
             
            
            if rows != None:
                for row in rows:
                    files.append(row[1])
                    
                    
            pipeline['files'] = files
            
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()

        return pipeline       
    
   def get_current_log_file_id(self):
        print("get_current_log_file_id")    
        conn = None
        cur = None
        log_file_id = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor() 
            statement = (
                "select log_file_id from  PIPELINES where status in ('RUNNING')"
            )
            cur.execute(statement, {})
            row = cur.fetchone()  
            log_file_id = row[0] 
            
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()

        return log_file_id
    
   def _condition(self, **kwargs):
       
       type = kwargs['type']
       
       if type == 'download':
           return " and download_file = 'Y' "
       elif type == 'decrypt':
           return " and decrypt_file = 'Y' "
       elif type == 'transfer':
           return " and transfer_file = 'Y' "
       else:
           return ''
       
       
   def stage_dmp_files(self, **kwargs):
       print("stage_dmp_files() ")
       log_file_id = None 
        
       conn = None
       cur = None
       try:
            if kwargs['log_file_id'] == None:
                log_file_id = self.get_current_log_file_id() 
            
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.DATA_STAGE_PKG.IMPORT_DMP_FILES',['I2B2_BLUE', log_file_id])
            conn.commit()
                    
       except cx_Oracle.DatabaseError as e: 
            raise
            
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()

     
   def get_files(self, **kwargs):
        print("get_files() ")
        files = [] 
        log_file_id = None

        
        conn = None
        cur = None
        try:
            if kwargs['log_file_id'] == None:
                log_file_id = self.get_current_log_file_id() 
            
            conn = self.get_db_connection()
            cur = conn.cursor() 
            condition = self._condition(**kwargs)
            statement = (
                "select log_file_id, dmp_file_name from  LOG_FILE_DMP_FILES where log_file_id in (:log_file_id) " +  condition
            )
            cur.execute(statement, {"log_file_id": log_file_id})
            rows = cur.fetchall()  
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