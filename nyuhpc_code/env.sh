# loads env and run command
module load python/gnu/3.6.5
module load spark/2.4.0
export PYTHON_PATH='/share/apps/python/3.6.5/bin/python'

spark-submit --conf spark.pyspark.python=$PYTHON_PATH

# execute like `$ ./env.sh "ds_reader.py"`