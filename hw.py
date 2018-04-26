import pyspark
if__name__=='__main__':
    sc=pyspark.SparkContext()
    data = '/data/share/bdm/nyc_restaurants.csv', use_unicode=False).cache()
    df=spark.read.load(data,
                  format = 'csv',
                  header=True,
                  inferScheme=True).cache()
    cDescription = df.na.drop(subset=['CUISINE DESCRIPTION'])
    cDescription = cDescription.groupBy('CUISINE DESCRIPTION').count().sort('count',ascending = False)
    print(cDescription)

    cDescription.rdd.saveAstextFile('output') 