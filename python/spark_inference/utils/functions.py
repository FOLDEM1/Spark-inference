from typing import Dict
import joblib
from pyspark.sql import functions as F , types as T
import pandas as pd
from pyspark.sql import DataFrame,SparkSession,DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamWriter
from utils.objects import ReadConfig, WatermarkConifg,WriteConfig,SparkConifgManager
import json
import pycm

###########################################################################################


def poll_data_stream(spark:SparkSession,config:ReadConfig,target_struct:T.StructType)->DataFrame:
    
    df_reader = spark.readStream.format(config.format)
    df = df_reader.options(**config.broker_config).load()
    df = (df.selectExpr('CAST(value as STRING) as payload ')
              .select(F.from_json(F.col('payload'),target_struct,{'mode':'PERMISSIVE',"allowNumericLeadingZeros": "true",}).alias('data'))
              .select('data.*')
              )
    return df 

def write_data_stream(df:DataFrame,config:WriteConfig)->DataStreamWriter:
        """
        
        """
        # NOTE : this function returns a writer NOT a streaming object meaning that
        # after the call , you should do DataStreamWriter.start()/save()
        # the reason is that you should decide when this writer writes 
        # ^__________^
        df = df.select(
            F.to_json(F.struct('*')).alias('value')
        )
        
        ds_writer = df.writeStream.format(config.format)

        ds_writer = (ds_writer.options(**config.broker_config)
                     .outputMode(config.outputMode))
        
        if config.trigger:
            ds_writer = ds_writer.trigger(**config.trigger)
        if config.foreachBatch_Callable:
            ds_writer.foreachBatch(config.foreachBatch_Callable)

        return ds_writer
            
def write_data(df:DataFrame,config:WriteConfig)->DataFrameWriter:
    
    df_writer = df.write.format(config.format).options(**config.broker_config)
    
    return df_writer

def clean_data(df:DataFrame)->DataFrame:
    df = df.dropna(how='any')
    return df


def get_config_from_json(path:str)->Dict[str,str]:
    # NOTE the "path" shoudl be like /path/reletaive/to/script/file.json 
    with open(path,'rt+') as f:
        config = json.load(f)
    return config

def create_spark_session(spark_config:SparkConifgManager)->SparkSession:
    
    spark = SparkSession.builder.remote(spark_config.spark_server) if spark_config.remote is True else SparkSession.builder.master(spark_config.spark_server)
    spark = spark.config(map=spark_config.config_options)
    spark = spark.appName(spark_config.appName).getOrCreate()
    return spark
    
def load_local_model(path_to_model:str):
    return joblib.load(path_to_model)

#######################################################################################################################

def merge_confusion_matricies(series:list)->str:
    # An aggreagtionb which
    matrix_dfs = [pd.DataFrame(json.loads(matrix))for matrix in series if matrix]
    
    running_agg = matrix_dfs[0]
    
    for matrix in matrix_dfs[1:]:    
        running_agg = running_agg.add(matrix,fill_value=0)
        
    return running_agg.fillna(0,inplace=True).to_json() 



def pycm_metric(matrix_json:str,metric:str)->Dict|None:
    
    # if the matrix was Null then there is no point of doing the math 
    if matrix_json is None:
        return {'value':None}
    
    sentinel = False # the flag which resolves a case when the confusion matrix consists of one class, for some metrics it might not make sense to calculate them 
    # but for the other it is possible but the pycm requires at least 2 classes, to manage that we add the sentinel/phantom class with count of 0, which later will be removed
    # so them metrics are possible to be calculated having only one class (if in that case it make sense)
    result:Dict[str,int]|None = None
    try:
        # we make it as dataframe so its easier to add coloumns and rows 
        pandas_df = pd.DataFrame(json.loads(matrix_json),dtype=int).fillna(0)
        
        row_classes = pandas_df.index
        columns_classes = pandas_df.columns

        if pandas_df.shape[0]!=pandas_df.shape[1]:
            # the matrix has to be square NxN, so we will add thoes missing rows and columns and fill them with values 0, because there was None of thoes presenet before
            rows_to_add = set(columns_classes).difference(row_classes)
            columns_to_add = set(row_classes).difference(columns_classes)
            
            pandas_df.loc[list(rows_to_add)]=0 # row wise fill
            pandas_df[list(columns_to_add)]=0 # column wise fill
        
        if pandas_df.shape ==(1,1):
            # the phantom class added to trick pycm for computing metrics
            pandas_df['_phantom_class']=0
            pandas_df.loc['_phantom_class']=0
            sentinel = True
            
        cm = pycm.ConfusionMatrix(matrix=pandas_df.to_dict(),metrics_off=False)
        result = getattr(cm,metric)
        if sentinel and isinstance(result,dict):
            # we pop the "phantom"
            result.pop("_phantom_class")
            
        if not isinstance(result,dict):
            # we return the dict all the time per class entry SO if we recived one value we mark it as {metric_name:value}
            if result is None or result == "None":
                # the pycm as none returns string 'None' so it has to be dealt with
                result = {metric : None}
            else:
                result = {metric: float(result)}
        else:
            # otherwise we force the key:val format which as str:float mapping
            result = {str(k):float(v) for k,v in result.items() if v is not None}    
        
    except Exception as e:
        # if there was an execption we return none
        result = {metric: None}
        return result
    
    return result


############

def stage_5_agg_confusion_matricies(series:pd.Series)->str:
    # NOTE: watch out where this funcion will be invoked , meaning the spark context, like "where" (the spark session avilable or ditributed) and "how" streaming / complete batch fashion
    # otheriwes it might either 1) throw an error, 2) do nothing and no reuslts will pop up 3) you might get lucky but next time you start it , it might not work
    
    matrix_dfs = [pd.DataFrame(json.loads(matrix))for matrix in series if matrix] # "if matrix" gives check if its not null for free :D 
    
    if not matrix_dfs:
        return "{}"
    
    running_agg = matrix_dfs[0]
    
    for matrix in matrix_dfs[1:]:    
        running_agg = running_agg.add(matrix,fill_value=0)
        
    return running_agg.fillna(0).to_json() # we have to fill the cells where in both or all matricies the fields were not present , in that case they would be Nan so we make them as 0/zero
    
