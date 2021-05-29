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
                "select a.id, a.log_file_path, a.log_file_name,a.created_at, b.status from PIPELINE_LOG_FILE a, PIPELINES b  where b.status = 'RUNNING' and a.id = b.log_file_id"
            )
            cur.execute(statement, {})
            row = cur.fetchone() 
            
            pipeline = {
                "id": row[0],
                "log_file_path": row[1],
                "log_file_name": row[2],
                "created_at": row[3],
                "status": row[4],
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
       
       
   def stage_custom_dmp_files(self, **kwargs):
       print("stage_custom_dmp_files() ")
        
       conn = None
       cur = None
       try:
            conn = self.get_db_connection()
            cur = conn.cursor()
            cur.callproc('I2B2_BLUE.DATA_LOAD_PKG.STAGE_CUSTOM_DMP_FILES',[kwargs['custom_log_file'], kwargs['custom_dmp_file'].split('.')[0]])
            conn.commit()
                    
       except cx_Oracle.DatabaseError as e: 
            raise
            
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()
                
   def stage_dmp_files(self, **kwargs):
       print("stage_dmp_files() ")
       log_file_id = None 
        
       conn = None
       cur = None
       try:
            if kwargs['log_file_id'] == None:
                log_file_id = self.get_current_log_file_id() 
                
                
            pipeline = self.get_current_pipeline()
            log_file_name = pipeline['log_file_name'] 
            
            conn = self.get_db_connection()
            cur = conn.cursor()
            cur.callproc('I2B2_BLUE.DATA_LOAD_PKG.STAGE_DATA',[log_file_name, log_file_name.split('.')[0]])
            conn.commit()
                    
       except cx_Oracle.DatabaseError as e: 
            raise
            
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()                
                                

   def load_data(self, **kwargs):
       print("load_data() ")
       log_file_id = None 
        
       conn = None
       cur = None
       try:
            if kwargs['log_file_id'] == None:
                log_file_id = self.get_current_log_file_id() 
                
                
            pipeline = self.get_current_pipeline()
            log_file_name = pipeline['log_file_name'] 
            
            conn = self.get_db_connection()
            cur = conn.cursor()
            cur.callproc('I2B2_BLUE.DATA_LOAD_PKG.LOAD_DATA',[log_file_name, log_file_name.split('.')[0]])
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
    
   def save_pipeline_log(self, log_file_path, log_file_name, log_data):
        print("save_pipeline_log(): log_file_name: {0}, {1}".format(log_file_path, log_file_name))
        
        conn = None
        cur = None
        try:
             
            conn = self.get_db_connection()
            cur = conn.cursor() 
            pu = PipelineUtils()
            statement = 'insert into PIPELINE_LOG_FILE(id, log_file_path, log_file_name) values (:id, :log_file_path,  :log_file_name)'
            cur.execute(statement, (pu.generate_uuid(), log_file_path, log_file_name))
             
             
            statement = (
                "select id, log_file_path, log_file_name from  PIPELINE_LOG_FILE where log_file_name in (:log_file_name)"
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
                
                
                
                
   def concept_dim_mapping_prep(self):
       print("OracleDataAccess::concept_dim_mapping_prep()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.concept_dim_mapping_prep',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()                
                
   def concept_dim_allergies(self):
       print("OracleDataAccess::concept_dim_allergies()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.allergies',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    
   def concept_dim_specimens(self):
       print("OracleDataAccess::concept_dim_specimens()")
       
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.specimens',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    

   def concept_dim_clinic_site(self):
       print("OracleDataAccess::concept_dim_clinic_site()")
    
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.clinic_site',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        

   def concept_dim_demographics(self):
       print("OracleDataAccess::concept_dim_demographics()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.demographics',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    

   def concept_dim_diagnosis(self):
       print("OracleDataAccess::concept_dim_diagnosis()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.diagnosis',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        

   def concept_dim_insurance_payors(self):
       print("OracleDataAccess::concept_dim_insurance_payors()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.insurance_payors',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    

   def concept_dim_laboratory_results(self):
       print("OracleDataAccess::concept_dim_laboratory_results()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.laboratory_results',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    

   def concept_dim_medications(self):
       print("OracleDataAccess::concept_dim_medications()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.medications',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    

   def concept_dim_notes(self):
       print("OracleDataAccess::concept_dim_notes()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.notes',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    

   def concept_dim_procedures(self):
       print("OracleDataAccess::concept_dim_procedures()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.procedures',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    

   def concept_dim_protocols(self):
       print("OracleDataAccess::concept_dim_protocols()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.protocols',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
    

   def concept_dim_services(self):
       print("OracleDataAccess::concept_dim_services()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.services',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()        
   

   def concept_dim_vital_signs(self):
       print("OracleDataAccess::concept_dim_vital_signs()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.vital_signs',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()
                
   def concept_dim_diagnosis_stg_update(self):
       print("OracleDataAccess::concept_dim_diagnosis_stg_update()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.diagnosis_stg_update',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()          
    
   def concept_dim_diagnosis_update(self):
       print("OracleDataAccess::concept_dim_diagnosis_update()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.diagnosis_update',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()
                
   def concept_dim_procedures_update(self):
       print("OracleDataAccess::concept_dim_procedures_update()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.procedures_update',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()
                
   def concept_dim_procedures_cd_load(self):
       print("OracleDataAccess::concept_dim_procedures_cd_load()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.procedures_cd_load',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()
                
   def concept_dim_other_mappings(self):
       print("OracleDataAccess::concept_dim_other_mappings()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.other_mappings',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()
                
   def update_concept_dimension(self):
       print("OracleDataAccess::update_concept_dimension()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.update_concept_dimension',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()  
                
   def concept_dimension_validations(self):
       print("OracleDataAccess::concept_dimension_validations()")
        
       conn = None
       cur = None
       try:   
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.CONCEPT_DIM_MAPPING_PKG.concept_dimension_validations',[])
            conn.commit() 
       except cx_Oracle.DatabaseError as e: 
            raise 
       finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()                 
                
   def recreate_bch_hpds_data(self):
        conn = None
        try:      
            conn = self.get_db_connection()
            cur = conn.cursor() 
            cur.callproc('I2B2_BLUE.HPDS_DATA_LOAD_PKG.RECREATE_BCH_HPDS_DATA',[])
            conn.commit()  
        except cx_Oracle.DatabaseError as e: 
            print(e)
            error, = e.args
            raise e  
        except Exception as e:
            print(e)
            error, = e.args
            raise e    
        finally:
            if not conn == None:
                conn.close()                                                                         
                
   def save_hpds_package_file_name(self, packed_file_name):
        print("save_hpds_package_file_name(): packed_file_name: {0}".format(packed_file_name))
        
        conn = None
        cur = None
        try:
             
            conn = self.get_db_connection()
            cur = conn.cursor() 
          
            statement = (
                "delete from PIPELINE_STATE where STATE_KEY in (:state_key)"
            )
            cur.execute(statement, {"state_key": 'HPDS_PACKED_FILE_NAME'})            

 
            statement = 'insert into PIPELINE_STATE(STATE_KEY, STATE_VALUE) values (:state_key, :state_value)'
            cur.execute(statement, ('HPDS_PACKED_FILE_NAME', packed_file_name))  
                         
            conn.commit()     
            
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()     
                
   def get_hpds_packed_file_name(self):
        print("get_hpds_packed_file_name()")
        file_name = None
        
        conn = None
        cur = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor() 
            statement = (
                "select STATE_KEY, STATE_VALUE from  PIPELINE_STATE where STATE_KEY in (:state_key)"
            )
            cur.execute(statement, {"state_key": 'HPDS_PACKED_FILE_NAME'})
            row = cur.fetchone() 
    
            if row != None:
                file_name = row[1]
                
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()

        return file_name
    
   def save_pipeline_state(self, **kwargs):
        print("OracleDataAccess::save_pipeline_state()")
        
        conn = None
        cur = None
        try:
             
            conn = self.get_db_connection()
            cur = conn.cursor() 
            
            
            for key, value in kwargs.items(): 
                statement = (
                    "delete from PIPELINE_STATE where STATE_KEY in (:state_key)"
                )
                cur.execute(statement, {"state_key": key})            
    
     
                statement = 'insert into PIPELINE_STATE(STATE_KEY, STATE_VALUE) values (:state_key, :state_value)'
                cur.execute(statement, (key, value))  
                         
            conn.commit()     
            
        except cx_Oracle.DatabaseError as e: 
            raise
            
        finally:
            if cur!=None:
                cur.close()
                
            if conn!=None:
                conn.close()    