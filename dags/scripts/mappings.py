"""
@author: anilkdegala
"""
import os
import subprocess, signal
from collections import OrderedDict
from scripts.oracle_data_access import OracleDataAccess
from scripts.configurations import *

class Mappings:
    
    def __init__(self):
        print("Mappings::__init__()")
        
        
    def concept_dim_mapping_prep(self):
        oda = OracleDataAccess()
        return oda.concept_dim_mapping_prep()
      
    def allergies(self):
        oda = OracleDataAccess()
        return oda.concept_dim_allergies()
    
    
    def specimens(self):
        oda = OracleDataAccess()
        return oda.concept_dim_specimens()
    

    def clinic_site(self):
        oda = OracleDataAccess()
        return oda.concept_dim_clinic_site()
    

    def demographics(self):
        oda = OracleDataAccess()
        return oda.concept_dim_demographics()
    

    def diagnosis(self):
        oda = OracleDataAccess()
        return oda.concept_dim_diagnosis()
    

    def insurance_payors(self):
        oda = OracleDataAccess()
        return oda.concept_dim_insurance_payors()
    

    def laboratory_results(self):
        oda = OracleDataAccess()
        return oda.concept_dim_laboratory_results()
    

    def medications(self):
        oda = OracleDataAccess()
        return oda.concept_dim_medications()
    

    def notes(self):
        oda = OracleDataAccess()
        return oda.concept_dim_notes()
    

    def procedures(self):
        oda = OracleDataAccess()
        return oda.concept_dim_procedures()
    

    def protocols(self):
        oda = OracleDataAccess()
        return oda.concept_dim_protocols()
    

    def services(self):
        oda = OracleDataAccess()
        return oda.concept_dim_services()
    

    def vital_signs(self):
        oda = OracleDataAccess()
        return oda.concept_dim_vital_signs() 
    
    def diagnosis_stg_update(self):
        oda = OracleDataAccess()
        return oda.concept_dim_diagnosis_stg_update()    
    
    def diagnosis_update(self):
        oda = OracleDataAccess()
        return oda.concept_dim_diagnosis_update()
    
    def procedures_update(self):
        oda = OracleDataAccess()
        return oda.concept_dim_procedures_update()
    
    def procedures_cd_load(self):
        oda = OracleDataAccess()
        return oda.concept_dim_procedures_cd_load()
    
    def other_mappings(self):
        oda = OracleDataAccess()
        return oda.concept_dim_other_mappings()
    
    def update_concept_dimension(self):
        oda = OracleDataAccess()
        return oda.update_concept_dimension()  
    
    def concept_dimension_validations(self):
        oda = OracleDataAccess()
        return oda.concept_dimension_validations()      
      
    
  