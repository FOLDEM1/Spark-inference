
from typing import Any, Callable, Dict,List
import joblib
from pyspark import SparkFiles
from typing_extensions import Self
from pyspark.sql import DataFrame,SparkSession
import psycopg
import pandas as pd
from pyspark.sql import functions as F , types as T
import pandas as pd

class WriteConfig:
    
    def __init__(self,config_dict:Dict[str,Any]) -> None:
        self.format = config_dict['format']
        self.outputMode=config_dict["outputMode"]
        self.broker_config = config_dict['broker_config']
        self.trigger:Dict[str,str] = config_dict['trigger'] if 'trigger' in config_dict.keys() else None
        self.foreachBatch_Callable = None
    def set_foreachBatch_Callable(self,callable:Callable):
        self.foreachBatch_Callable=callable

class PostgresConnector:
    def __init__(
        self,host:str,port:int|str,user:str,db:str,table:str,password:str,
        spark:SparkSession,
        kafka_topic:str,kafka_bootstrap_servers:str,
        window_at_write:str='60 seconds'
        ) -> None:
        self.__host:str = host 
        self.__user:str=user 
        self.__db:str = db 
        self.__table:str = table
        self.__password:str=password
        self.__port:int|str = port
        self.spark:SparkSession=spark
        self.__kafka_topic=kafka_topic
        self.__kafka_bootstrap_servers=kafka_bootstrap_servers
        
        self.__Q1 = f"""
                    INSERT INTO {self.__table} (batch_id,start_time,end_time)
                    values (%s,%s,%s)
                    ON CONFLICT(batch_id)
                    DO UPDATE SET start_time=EXCLUDED.start_time,end_time=EXCLUDED.end_time ;
                    """
        self.__S1 = f' SELECT count(batch_id) FROM {self.__table} where batch_id=%s and start_time is not null and end_time is not null'
        self.__D1 = f'DELETE FROM {self.__table} where batch_id=%s'
        self.__window_at_write = window_at_write
    def __call__(self,batch_df:DataFrame,batch_id:int) -> None:
        
        batch_df.cache()
        try:
            with psycopg.connect(f'postgresql://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__db}') as conn:
                with conn.cursor() as cur:
                    row_count = cur.execute(self.__S1,(batch_id,)).fetchall()[0]
                    
                    if row_count ==1:
                        return
                    else:
                        cur.execute(self.__D1,(batch_id,))
                        
                    row_count = batch_df.count()
                    
                    if row_count==0:
                        return
                    
                    agged_df = batch_df.groupBy(
                            F.window('time_stamp',self.__window_at_write), 
                            'true_label', 
                            'prediction'
                        ).count()
                    
                    time_range = (agged_df.
                                  select(F.min('window.start').alias('start'),F.max('window.end').alias('end'))
                                  .collect()[0])
                    # so the both class an labels are integer strins
                    agged_df = agged_df.groupBy(F.col('window.start').alias('window_start'),F.col('window.end').alias('window_end'),'true_label').agg(
                        F.map_from_entries(F.collect_list(F.struct(F.col('prediction').cast(T.IntegerType()).cast(T.StringType()),'count'))).alias('m1')
                    )
                    agged_df = agged_df.groupBy('window_start','window_end').agg(
                            F.map_from_entries(F.collect_list(F.struct('true_label','m1'))).alias('confusion_matrix')
                    )

                    agged_df = agged_df.select(F.to_json(F.struct('*')).alias('value'))

                    (agged_df.write.format('kafka')
                        .option('kafka.bootstrap.servers',self.__kafka_bootstrap_servers)
                        .option('topic',self.__kafka_topic)
                        .save())

                    cur.execute(self.__Q1,(batch_id,time_range.start,time_range.end))
        except Exception as e:
            print(f'errro in postgress connector :C {e}')
            raise
        finally:
            batch_df.unpersist()
            

class ReadConfig:
    
    def __init__(self,config_dict:Dict[str,Any]) -> None:
        self.format = config_dict['format']
        self.broker_config = config_dict['broker_config']
    
        
class WatermarkConifg:
    
    def __init__(self,config:Dict[str,Any]) -> None:
    
        self.base_watermark=config['watermark']
        self.network_delay = config['network_delay'] if 'network_delay' in config.keys() else 0
        self.time_metric = config["time_metric"]
        
    @property
    def watermark(self)->str:
        return f"{self.base_watermark + self.network_delay} {self.time_metric}"
    
class SparkConifgManager:
    
    def __init__(self,config_dict:Dict[str,Any]) -> None:
        
        self.appName = config_dict['appName'],
        self.remote= config_dict['remote']
        self.spark_server = config_dict["spark_server"]
        self.config_options = config_dict["config_options"]
        
class Model_Singleton:
    
    __model_instance = None 
  
    def __new__(cls,*args,**kwargs) -> Self:
        if cls.__model_instance is None:
            cls.__model_instance = super().__new__(cls)
        return cls.__model_instance
    
    def __init__(self,param_dict:Dict[str,Any]|None=None,model_loader_func:Callable|None=None) -> None:
        if Model_Singleton.__model_instance is not None:
            raise Exception("Singleton was created")
        if model_loader_func is None:
            raise Exception("The loader func was empty :C")
        self.model = model_loader_func(**param_dict)
    
    def predict(self,*args,**kwargs)->List[float]|Dict[str|int|float,str|int|float]:
        return self.model.predict(*args,**kwargs)
        
class SparkSharedModel:
    def __init__(self,path_to_model) -> None:
        self.model_path = path_to_model
        self.model = None
    def predict(self,pdf:pd.Series) -> pd.Series:
        if self.model is None:
            path = SparkFiles.get(self.model_path)
            self.model = joblib.load(path)
        pdf = pdf.drop(columns=['id','time_stamp'],errors='ignore')
        return pd.Series(self.model.predict(pdf))

        
        
        
        
