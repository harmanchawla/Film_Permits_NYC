# Output 1: "Distribution of film permits by borough"
'''
	+-------------+-----+                                                           
	|      Borough|count|
	+-------------+-----+
	|Staten Island|  883|
	|        Bronx| 1889|
	|       Queens| 9817|
	|     Brooklyn|19238|
	|    Manhattan|30957|
	+-------------+-----+
'''

# Output 2: "Distibution of film permits by type"
'''
	+--------------------+-----+
	|           EventType|count|
	+--------------------+-----+
	|DCAS Prep/Shoot/W...|  749|
	|      Rigging Permit| 1474|
	|Theater Load in a...| 5774|
	|     Shooting Permit|54787|
	+--------------------+-----+
'''

# Output 3: Distibution by Borough and EventType
'''
	+-------------+--------------------+-----+                                      
	|      Borough|           EventType|count|
	+-------------+--------------------+-----+
	|    Manhattan|     Shooting Permit|24905|
	|     Brooklyn|     Shooting Permit|17762|
	|       Queens|     Shooting Permit| 9566|
	|    Manhattan|Theater Load in a...| 4711|
	|        Bronx|     Shooting Permit| 1688|
	|     Brooklyn|Theater Load in a...| 1040|
	|    Manhattan|      Rigging Permit|  939|
	|Staten Island|     Shooting Permit|  866|
	|    Manhattan|DCAS Prep/Shoot/W...|  402|
	|     Brooklyn|      Rigging Permit|  331|
	|       Queens|      Rigging Permit|  165|
	|        Bronx|DCAS Prep/Shoot/W...|  159|
	|     Brooklyn|DCAS Prep/Shoot/W...|  105|
	|       Queens|DCAS Prep/Shoot/W...|   72|
	|        Bronx|      Rigging Permit|   34|
	|       Queens|Theater Load in a...|   14|
	|Staten Island|DCAS Prep/Shoot/W...|   11|
	|        Bronx|Theater Load in a...|    8|
	|Staten Island|      Rigging Permit|    5|
	|Staten Island|Theater Load in a...|    1|
	+-------------+--------------------+-----+
	'''

# Output 4: average duration of permits by type

'''
	+--------------------+------------------+                                       
	|           EventType|      Avg Duration|
	+--------------------+------------------+
	|Theater Load in a...|49.667116961090066|
	|      Rigging Permit|21.643973315241986|
	|     Shooting Permit|16.713517501110445|
	|DCAS Prep/Shoot/W...|14.702136181575431|
	+--------------------+------------------+

	'''

# Output 5: average duration of permits by borough
'''
    +-------------+------------------+
	|      Borough|      Avg Duration|
	+-------------+------------------+
	|    Manhattan| 23.56061849231788|
	|       Queens|16.278966758344335|
	|     Brooklyn|16.241295006411036|
	|        Bronx| 16.00328215987296|
	|Staten Island|15.311004152510383|
	+-------------+------------------+
'''