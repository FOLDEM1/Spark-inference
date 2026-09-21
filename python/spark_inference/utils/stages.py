from utils.functions import poll_data_stream,clean_data
from pyspark.sql import types as T,SparkSession,DataFrame
from pyspark.sql import functions as F
from utils.objects import ReadConfig,WatermarkConifg
from utils.functions import poll_data_stream
def stage1(spark:SparkSession,target_struct:T.StructType,read_config:ReadConfig,watermark:str="30 seconds")->DataFrame:
    
    df = poll_data_stream(spark=spark,config=read_config,target_struct=target_struct)
    df = clean_data(df=df)
    df = df.withWatermark('time_stamp',f"{watermark}")
    return df 

def stage2(spark:SparkSession,config:ReadConfig,target_struct,watermark:str="30 seconds")->DataFrame:
    df = poll_data_stream(spark,config=config,target_struct=target_struct)
    df = clean_data(df)
    df = df.withWatermark('time_stamp',f'{watermark}')

    return df

def stage3(df_s1:DataFrame,df_s2:DataFrame,time_diff_in_seconds:int=10)->DataFrame:
    
    df_s1,df_s2 = df_s1.alias('df1'),df_s2.alias('df2')

    df_s3 = df_s1.join(
            other=df_s2,
            on=[
                df_s1.id==df_s2.id ,
                df_s2.time_stamp  >=df_s1.time_stamp , 
                df_s2.time_stamp - df_s1.time_stamp <= F.expr(f"INTERVAL {time_diff_in_seconds} SECONDS") 
            ],
            how='inner'
        )
    
    df_s3 = df_s3.select(F.col('df1.time_stamp').alias('time_stamp'),F.col('df1.prediction'),F.col('df2.true_label'))
    return df_s3

def stage5(df:DataFrame,wmc:WatermarkConifg)->DataFrame:
    df = df.withWatermark('window_start',wmc.watermark)
    return df
