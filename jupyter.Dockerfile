FROM public.ecr.aws/b1m7t7i1/pyspark-granica-demo:v1

RUN pip install --no-cache-dir jupyter==1.1.0

CMD jupyter notebook --no-browser --ip=0.0.0.0 --allow-root