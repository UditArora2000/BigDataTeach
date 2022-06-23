import findspark
findspark.init() 

from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/pullreq.bda") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/pullreq.bda") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .master("spark://localhost:7077") \
	.config('spark.executor.cores', '4') \
    .getOrCreate()

pp1a = [
	{ 
		'$match' : { 'status': 'opened'} 
	},
    { '$group' :
        {
          '_id': '$date',
          'count': { '$sum': 1 }
        }
    },
    {
    	'$sort': {'_id': 1}
    }
   ]

pp1b = [
	{ 
		'$match' : { 'status': 'discussed'} 
	},
    { '$group' : 
        {
          '_id': '$date',
          'pids': {'$addToSet': '$pid'}
        }
    },
    {
    	'$addFields': {'count': {'$size': '$pids'}}
    },
    {
    	'$sort': {'_id': 1}
    },
    {
    	'$unset': 'pids'
    }
   ]

pp2 = [
	{
		'$match' : {'status': 'discussed'}
	},
	{
		'$group': {
			'_id': {
					'month': {'$month': {'$dateFromString': {'dateString': '$date'}}},
					'name': '$name'
				   },
			'count': {'$sum': 1}
			}
	},
	{
		'$sort': {'count': -1}
	},
	{
		'$group': { '_id' : '$_id.month',
					'count': {'$first': '$count'},
					'name': {'$first' : '$_id.name'}
			}
	},
	{
    	'$sort': {'_id': 1}
    }
   ]

pp3 = [
	{
		'$match' : {'status': 'discussed'}
	},
	{
		'$group': {
			'_id': {
					'year': {'$year': {'$dateFromString': {'dateString': '$date', 'timezone': '+0530'}}},
					'week': {'$week': {'$dateFromString': {'dateString': '$date', 'timezone': '+0530'}}},
					'name': '$name'
				   },
			'count': {'$sum': 1}
			}
	},
	{
		'$sort': {'count': -1}
	},
	{
		'$group': { '_id' : {
						'week': '$_id.week', 
						'year': '$_id.year'
					},
					'count': {'$first': '$count'},
					'name': {'$first' : '$_id.name'}
			}
	},
	{
    	'$sort': {'_id.year': 1, '_id.week': 1}
    }
   ]

pp4 = [
	{
		'$match' : {'status': 'opened'}
	},
	{
		'$group': {
			'_id': {
					'year': {'$year': {'$dateFromString': {'dateString': '$date', 'timezone': '+0530'}}},
					'week': {'$week': {'$dateFromString': {'dateString': '$date', 'timezone': '+0530'}}}
					},
			'count': {'$sum': 1} 
			}
	},
    {
    	'$sort': {'_id.year': 1, '_id.week': 1}
    }
	]

pp5 = [
	{
		'$addFields': {'year': {'$year': {'$dateFromString': {'dateString': '$date', 'timezone': '+0530'}}},
					   'month': {'$month': {'$dateFromString': {'dateString': '$date', 'timezone': '+0530'}}}
		}
	},
	{
		'$match': {'status': 'merged', 'year': 2010}
	},
	{ '$group' : 
        {
          '_id': '$month',
          'pids': {'$addToSet': '$pid'}
        }
    },
    {
    	'$addFields': {'count': {'$size': '$pids'}}
    },
    {
    	'$sort': {'_id': 1}
    },
    {
    	'$unset': 'pids'
    }
	]

pp6 = [
	{
		'$group': 
			{
				'_id': '$date',
				'count': {'$sum': 1}
			}
	},
	{
		'$sort': {'_id': 1}
	}
	]

pp7 = [
	{
		'$addFields': {
			'year': {'$year': {'$dateFromString': {'dateString': '$date', 'timezone': '+0530'}}}
		}
	},
	{
		'$match': {'status': 'opened', 'year': 2011}
	},
	{
		'$group': {
			'_id': '$name',
			'count': {'$sum': 1}
		}
	},
	{
		'$sort': {'count': -1}
	},
	{
		'$limit': 1
	}
	]

start_time = time.time()

df1a = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
	.option("pipeline", pp1a).load()

df1b = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
	.option("pipeline", pp1b).load()

df2 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
	.option("pipeline", pp2).load()

df3 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
	.option("pipeline", pp3).load()

df4 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
	.option("pipeline", pp4).load()

df5 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
	.option("pipeline", pp5).load()

df6 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
	.option("pipeline", pp6).load()

df7 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
	.option("pipeline", pp7).load()

print(f"Computation time: {time.time() - start_time}")

df1a.show()
df1b.show()
df2.show()
df3.show()
df4.show()
df5.show()
df6.show()
df7.show()