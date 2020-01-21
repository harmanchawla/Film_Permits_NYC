
from pyspark.sql import SparkSession

import time
import sys
import json
import numpy as np

from typing import List


def timed(fn, files: List[str]=None) -> None:
    """
    a standardized interface for timing some function fn which accepts a df and potentially other args and kwargs. 
    it calculates runtimes and avg runtimes while iterating through the df generator and calling fn, and prints them.
    """
    spark = SparkSession.builder.getOrCreate()  # init spark

    # APPENDED CODE
    spark.sparkContext.setLogLevel("ERROR") # Warnings cluttered stdout
    run_time_record = np.array([])
    average_time_record = np.array([])
    # ---

    time_start_all = time.time()
    time_end = time_start_all
    total_runtime = 0
    total_runtime_load = 0
    max_runtime = 0
    max_output = None

     # CHANGED FUNCTION CALL
    output = fn(spark, df, master_itemset, i+1)
        

        # try:
        #     output = fn(df)
        # except Exception as e:
        #     print(e)
        #     continue
    time_end = time.time()

    # specific to current ds
    runtime = time_end - time_start
    print("actual runtime:", runtime)  # DEBUG
    total_runtime += runtime

    # general for all ds
    total_runtime_load = time_end - time_start_all
    print('running avg runtime (including load):', total_runtime_load / actual_len_dfs)

    # APPENDED CODE: keep a track of run times for visualization later
    run_time_record = np.append(run_time_record, [runtime])
    average_time_record = np.append(average_time_record, [total_runtime_load / actual_len_dfs])
        # ----

        if runtime > max_runtime:
            max_runtime = runtime
            max_output = output

        if actual_len_dfs >= limit:
            break

    total_runtime_load = time_end - time_start_all
    # --- 


    print("\n\n")
    print("### SUMMARY ###")
    print("max runtime:", max_runtime)
    
    if max_output is not None:
        print("max output:", max_output)

    print('# RUNTIME (ONLY FOR FN) #')
    print('total runtime:', total_runtime)
    print('total avg runtime:', total_runtime / actual_len_dfs)

    print('# RUNTIME (INCLUDING LOADS AND INITIAL DIR PARSING) #')
    print('total runtime:', total_runtime_load)
    print('total avg runtime:', total_runtime_load / actual_len_dfs)
