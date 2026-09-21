FROM apache/spark:4.2.0
USER root
WORKDIR /app
ARG MODEL_NAME
## we copy needed files from uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# we set the default apkcage isntall dir 
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python

# we copy the requirements
COPY ./dockerfiles/requirements/v1/req.txt /tmp/req.txt

# we install the python interpeter, then we activate the venv, then we install the need modules
RUN uv python install 3.11 && \
    uv venv /opt/venv --python 3.11 && \
    . /opt/venv/bin/activate && \
    uv pip install --no-cache-dir -r /tmp/req.txt

    # we set needed perimssion for all specified files 
RUN chmod -R a+rX /opt/venv /opt/uv && \
    chown -R spark:spark /opt/venv /opt/uv

# NOTE: 
# remember that in this project we also use binding for utils
# so if you want to pass the "ready" utils and want to avoid thoes binds then use COPY for thoes as well like
# COPY /path/to/utils ... and adjust it 
# COPY ./models/${MODEL_NAME:-.} /app/models/${MODEL_NAME:-.}


#we set the interpreter path for container
ENV PATH="/opt/venv/bin:${PATH}"
# we set the python module search path 
ENV PYTHONPATH="/app/lib:${PYTHONPATH}" 
# we set the interpeter for spark application
ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

USER spark

