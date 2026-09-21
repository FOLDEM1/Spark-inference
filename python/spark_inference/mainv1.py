
from utils.stages import stage1, stage2, stage3 ,stage5
from utils.functions import get_config_from_json,create_spark_session, poll_data_stream, pycm_metric, write_data_stream
from utils.functions import merge_confusion_matricies
from utils.objects import PostgresConnector, ReadConfig, SparkConifgManager,SparkSharedModel, WatermarkConifg, WriteConfig
from dotenv import load_dotenv
from pyspark.sql import functions as F , types as T 
import os
from utils.structs import *
import pathlib
import sys

## ENV
q=load_dotenv(os.getenv("SPARK_PIPELINE_ENV_FILE"))
print(os.getenv("SPARK_PIPELINE_ENV_FILE"))
print("the value of q is ",q)
if int(os.getenv("USING_WINDOWS")) == 1:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
##

## SPARK
spark_config_path = os.getenv('SPARK_CONFIG_JSON_PATH')
spark_config_dict = get_config_from_json(spark_config_path)
spark_config = SparkConifgManager(spark_config_dict)
spark = create_spark_session(spark_config=spark_config)


##MODEL 
model_path = pathlib.Path(os.getenv('MODEL_PATH'))
spark.addArtifact(os.getenv('MODEL_PATH'),file=True)

predictor = SparkSharedModel("/app/lib/models/sgd_wine_classifier.pkl")
predict_udf = F.pandas_udf(predictor.predict,returnType=T.FloatType())


def main():
    global spark
    wm_config_dict = get_config_from_json(os.getenv('WATERMARK_CONFIG'))
    
    #########
    stage_1_read_config = ReadConfig(get_config_from_json(os.getenv('STAGE_1_READ')))
    stage_1_wm_conifg = WatermarkConifg(wm_config_dict['stage_1'])
    
    df_stage_1 = stage1(spark=spark,read_config=stage_1_read_config,target_struct=STAGE_1_READ_STRUCT,watermark=stage_1_wm_conifg.watermark)
    df_stage_1 = df_stage_1.withColumn('prediction',predict_udf(F.struct('*')))
    ############
    stage_2_read_config  = ReadConfig(get_config_from_json(os.getenv('STAGE_2_READ')))
    stage_2_wm_config = WatermarkConifg(wm_config_dict['stage_2'])
    df_stage_2 = stage2(spark,stage_2_read_config,target_struct=STAGE_2_READ_STRUCT,watermark=stage_2_wm_config.watermark)
    #############
    stage_3_write_config = WriteConfig(get_config_from_json(os.getenv('STAGE_3_WRITE')))

    df_stage_3 = stage3(df_stage_1,df_stage_2)
    ##############
    stage_4_write_config = WriteConfig(get_config_from_json(os.getenv('STAGE_4_WRITE')))
    stage_4_db_config = get_config_from_json(os.getenv("STAGE_4_DB_CONFIG"))
    
    pgc = PostgresConnector(**stage_4_db_config,spark=spark,window_at_write=str(os.getenv('STAGE_4_WINDOW')),kafka_topic=stage_4_write_config.broker_config['topic'],kafka_bootstrap_servers=stage_4_write_config.broker_config["kafka.bootstrap.servers"])
    
    #stage 4 is forahcbacth :D
    
    query_4 =(df_stage_3.writeStream.
              format(stage_4_write_config.format).
              option('checkpointLocation',stage_4_write_config.broker_config['checkpointLocation'])
              .trigger(**stage_4_write_config.trigger)  
              .foreachBatch(pgc)
              .outputMode(stage_4_write_config.outputMode)
              ).start()

    ## stage_5
    
    stage_5_read_config = ReadConfig(get_config_from_json(os.getenv('STAGE_5_READ'))) 
    stage_5_wm = WatermarkConifg(wm_config_dict['stage_5'])
    stage_5_write_config = WriteConfig(get_config_from_json(os.getenv('STAGE_5_WRITE')))
    df_stage_5 = poll_data_stream(spark=spark,config=stage_5_read_config,target_struct=STAGE_5_READ_STRUCT)
    
    
    df_stage_5 = stage5(df_stage_5,stage_5_wm)
    matricies_agg = F.udf(merge_confusion_matricies,returnType=T.StringType())
    df_stage_5 = df_stage_5.groupBy(F.window('window_start',str(os.getenv('STAGE_5_WINDOW'))).alias('W')).agg(F.collect_list(F.col('confusion_matrix')).alias('aggs'))
    """
    RULE OF THUMB -> if doing state-aggs -> Collect list + F.udf-> func has to take then the list as parameter
                    IF i gave whole complte no moving bacth then i can use pandas wrappe F.pandas_uds
                    otheriwse it might break  
    """
    
    df_stage_5 = df_stage_5.select(F.col('W.start').alias('window_start'),F.col('W.end').alias('window_end'),F.col('aggs'))
    df_stage_5 = df_stage_5.withColumn('confusion_matrix',matricies_agg(F.col('aggs'))).drop('aggs')
    
    query_5 = write_data_stream(df_stage_5,stage_5_write_config).start()
    
    ## stage_6
    
    stage_6_read_config = ReadConfig(get_config_from_json(os.getenv('STAGE_6_READ')))
    stage_6_write_config = WriteConfig(get_config_from_json(os.getenv('STAGE_6_WRITE')))
    stage_6_metrics = get_config_from_json(os.getenv('STAGE_6_DESIRED_METRICS_JSON_PATH'))
    
    df_stage_6 = poll_data_stream(spark,stage_6_read_config,STAGE_6_READ_STRUCT)
    
    metric_func = F.udf(pycm_metric,returnType=T.MapType(T.StringType(),T.FloatType()))
    stage_6_metrics_func_dict = {metric:metric_func('confusion_matrix',F.lit(metric)) for metric in stage_6_metrics['metrics']}
    
    df_stage_6 = df_stage_6.withColumns(stage_6_metrics_func_dict)
    df_stage_6 =df_stage_6.drop(F.col('confusion_matrix'))
    
    query_6 = write_data_stream(df_stage_6,stage_6_write_config).start()
   
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    print('BEGIN')
    try:
        main()
    except Exception as e:
        print(f" there was an exception (@__@;) \n{e}")
    finally:
        for stream in spark.streams.active:
            stream.stop()
        print('BYE BYE :D')
