import uuid
import os
 
class PipelineUtils:
    def __init__(self):
        print("PipelineUtils::__init__()")
        
    def generate_uuid(self):
        return str(uuid.uuid4()) 
             
  
   