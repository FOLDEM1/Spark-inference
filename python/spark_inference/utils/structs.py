from pyspark.sql import types as T

STAGE_1_READ_STRUCT = T.StructType([
    T.StructField("id", T.StringType() , False),
    T.StructField("Alcohol", T.StringType(), False),
    T.StructField("Malic.acid", T.StringType(), False), 
    T.StructField("Ash", T.StringType(), False),
    T.StructField("Acl", T.StringType(), False),
    T.StructField("Mg", T.StringType(), False),
    T.StructField("Phenols", T.StringType(), False),
    T.StructField("Flavanoids", T.StringType(), False),
    T.StructField("Nonflavanoid.phenols", T.StringType(), False), 
    T.StructField("Proanth", T.StringType(), False),
    T.StructField("Color.int", T.StringType(), False), 
    T.StructField("Hue", T.StringType(), False),
    T.StructField("OD", T.StringType(), False),
    T.StructField("Proline", T.StringType(), False),
    T.StructField("time_stamp", T.TimestampType(), False)
])

STAGE_2_READ_STRUCT = T.StructType([
    T.StructField('id',T.StringType(),False),
    T.StructField('true_label',T.StringType(),False),
    T.StructField('time_stamp',T.TimestampType(),False)
])

STAGE_5_READ_STRUCT = T.StructType([
    T.StructField('window_start',T.TimestampType(),nullable=False),
    T.StructField('window_end',T.TimestampType(),nullable=False),
    T.StructField('confusion_matrix',T.StringType(),nullable=False)
])

STAGE_6_READ_STRUCT = T.StructType([
    T.StructField('window_start',T.TimestampType(),nullable=False),
    T.StructField('window_end',T.TimestampType(),nullable=False),
    T.StructField('confusion_matrix',T.StringType(),nullable=False)
])