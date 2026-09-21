# Spark-inference
This projects consists of docker compose + dockerfiles, spark pipeline , the simple model for classification wine dataset, used dataset + simple sample generator which sends the samples to the broker which later are read by the pipeline.

This workflow models the system when the producer like user IoT device etc generates sample which are sent to broker AND later we anticipate that the labels for given sample will arrive. 

The pipeline will read the samples and make a prediction using the passed model. Then it will join the true label with prediction and stote it in the topic passed in config. After that the records as (true label,prediction,window_start), will be aggregated into the confusion matricies, starting with short interval like 1 minute and later into larger one f.e 5 minutes, 15 minutes. When the aggregates are complete, the pipeline computes the matrix which are possible to derive from confusion matrix using the pycm module. The metrics can be specified in the metrics.json file in configs. Metrics then are places into the final topic which later can be read and used, f.e logging with ml-flow, creating dashboards and many more.

There is plenty of room for adjustmetns and changes using the *.env files,configs and by changing the code to adjust it for user needs. However this version uses the model/models passed with docker. For small models it is managable , altough additional container serving the model like BentoML, MLflow could be more preferable, but the spark workers in that case has to make requests for prediction.


# Example use case: 
given the project layout , this command should build the spark images and spin up the containers with all needed env variables :

 <b> <i>docker compose --profile all --env-file ..\envs\spark_connect.env --env-file ..\envs\spark_master.env --env-file ..\envs\spark_shared.env --env-file ..\envs\spark_worker.env  --env-file ../envs/docker_configs.env 
 -f .\docker-composev1.yaml up -d </i> </b>


the main program for pipeline should start with simple: <b><i>uv run mainv1.py</b></i>

while the kafka producer with f.e: <b><i>  "uv run .\main.py -b localhost:9091 -t ml-data -d 0 -c ml-labels -p ..\..\data\wine100\val\val.csv\" </i></b> 

 
