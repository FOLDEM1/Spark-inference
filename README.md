# Spark-inference
This projects consists of docker compose + dockerfiles, spark pipeline , the simple model for classification wine dataset, used dataset + simple sample generator which sends the samples to the broker which later are read by the pipeline

given the project layout , this command should build the spark images and spin up the containers with all needed env variables :

 <b> <i>docker compose --profile all --env-file ..\envs\spark_connect.env --env-file ..\envs\spark_master.env --env-file ..\envs\spark_shared.env --env-file ..\envs\spark_worker.env  --env-file ../envs/docker_configs.env 
 -f .\docker-composev1.yaml up -d </i> </b>


the main program for pipeline should start with simple: <b><i>uv run mainv1.py</b></i>

while the kafka producer with f.e: <b><i>  uv run .\main.py -b localhost:9091 -t ml-data -d 0 -c ml-labels -p ..\..\data\wine100\val\val.csv\ </i></b> 

 
