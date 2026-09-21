import time
import datetime
from typing import Any,List,Callable,Dict
import linecache
import random
import json
import faker
import numpy
from confluent_kafka import Producer

class SamplePoller:
    
    def __init__(self,path_to_dataset,separator=',',desired_class=dict,degrade_factor=0) -> None:
        self.path = path_to_dataset
        self.desired_class = desired_class
        self.num_of_lines = -1
        self.columns:None|List = None
        self.separator = separator
        self.degrade_factor:float = float(degrade_factor)
        
        self.__post_init__()
    def __post_init__(self):
        
        try:
            with open(self.path,'r') as f:
                self.num_of_lines = sum(1 for line in f )
            
            self.columns = linecache.getline(self.path,1).strip().split(self.separator)
        except Exception as e :
            raise Exception("The path to file was not correct check it again")
        
            
    def poll_one(self,index=None)->Any:
        if index is None:
            index = random.randint(2,self.num_of_lines-1)
        
        line:List = linecache.getline(self.path,min(max(1,index),self.num_of_lines-1)).strip().split(self.separator)
        linecache.clearcache()
        line = [l if numpy.random.random() > self.degrade_factor else None for l in line ]
        target:Dict = {key:val for key,val in zip(self.columns,line)}
        
      
        
    
        target = self.desired_class(**target)
        
        return target
    
    
class KafkaProducer:
    
    def __init__(self,config:Dict[str,str],id:str="1",class_topic:str|None=None,target_column:str='target') -> None:
        __slots__ = ("config","id","producer","dataFunc")
        self.config = config
        self.id=id
        self.producer:Producer|None = None
        self.poller:SamplePoller|None =None
        self.class_topic:str|None =  class_topic
        self.target_column = target_column
        # setattr(object,key,val) SET
        # getattr(object,key) GET
        # hasattr(object,key) CHECK
        self.__faker= faker.Faker()
        print(self.__dict__)
        self.__post_init__()
        
    def __post_init__(self):
        for test in ["topic","bootstrap.servers"]:
            if test not in self.config:
                raise Exception(f'The object with id = {self.id} does not contatin the key {test} ')
        self.producer = Producer({'bootstrap.servers':self.config['bootstrap.servers']})
    def setPoller(self,pollerObject):
        self.poller=pollerObject
    def produce(self,*args,**kwargs):
        if self.poller is None:
            raise Exception("The poller was not set ")
        data:Dict|Any = self.poller.poll_one(*args,**kwargs)
        
        
            
        data = dict(data)  
        temp_id = self.__faker.uuid4()
        if 'id' not in data.keys():
            data['id'] = temp_id
        
        if self.target_column not in data.keys():
            raise Exception(f'the provided target columns => {self.target_column} was not present in the data read check them both if needed')

        if 'time_stamp' not in data.keys():
            data['time_stamp'] =  str(datetime.datetime.now() + datetime.timedelta(seconds=numpy.random.randint(-15,15)))
        temp_dict = {
            'id' :data['id'],
            'true_label':data[self.target_column],
            'time_stamp': str(datetime.datetime.now() + datetime.timedelta(seconds=numpy.random.randint(-15,15)))
        }
        data.pop(self.target_column)
        
        self.producer.produce(topic=self.config['topic'],value=json.dumps(data).encode('utf-8'))
        time.sleep(3)
        self.producer.produce(topic=self.config['class.topic'],value=json.dumps(temp_dict).encode('utf-8'))
        print('===================')
        print(data)
        print('--------------------')
        print(temp_dict)
        print('********************')
        
    def __del__(self):
        if self.producer:
            self.producer.flush()