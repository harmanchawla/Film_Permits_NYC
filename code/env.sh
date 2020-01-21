# loads env and run command
module load python/gnu/3.6.5
module load spark/2.4.0
export PYTHON_PATH='/share/apps/python/3.6.5/bin/python'

spark-submit --conf spark.pyspark.python=$PYTHON_PATH $1

# execute like `$ ./env.sh "task_driver.py"`