import csv
import sys
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
## Import PySpark SQL Functions model for aggregation:
import pyspark.sql.functions as F


def main(sc):
    ## Copy and Paste the code from the notebook here

    # ## Initialization Spark:
    # sc = SparkContext()
    # spark = SparkSession(sc)
    # sc


    ## Define the path of input dataset:
    # filePath_input = "complaints_sample(1).csv"   ## This is how to read a local file in Notebook.

    ## For every invocation of Python, sys.argv is automatically a list of strings
    ## representing the arguments (as separated by spaces) on the command-line.
    filePath_script = sys.argv[0]   ## The execuatable script is always the first argument in the command.
    filePath_input = sys.argv[1]    ## The input file is the second argument in the command.

    ## Read CSV file into PySpark SQL DataFrame:
    df = spark.read.csv(filePath_input,
                        sep=",",
                        ## Set comma , (default) as the separator.

                        quote="\"",
                        ## Set quotation mark " (default) as the character to escape the comma inside quotes.
                        #                     """
                        #                     quote – sets a single character used for escaping quoted
                        #                     values where the separator can be part of the value.
                        #                     If None is set, it uses the default value, ".
                        #                     If you would like to turn off quotations, you need to set
                        #                     an empty string.
                        #                     """

                        escape="\"",
                        ## Set quotation mark" as the character to escape nested quotes.
                        #                     """
                        #                     escape – sets a single character used for escaping quotes
                        #                     inside an already quoted value.
                        #                     If None is set, it uses the default value, \.
                        #                     """

                        multiLine=True,
                        ## Fix the csv file reading issue as some records span multiple lines.
                        #                     """
                        #                     multiLine – parse records, which may span multiple lines.
                        #                     If None is set, it uses the default value, false.
                        #                     """

                        header=True,
                        ).cache()

    ## Create a new DataFrame with only the columns needed for the problem:
    complaints_df = df.select("Product", "Date Received", "Company")

    ## Generate a result DataFrame of 'Product' count by 'Year':
    result1 = complaints_df.groupBy(
        'Product',
        F.year('Date received').alias('Year')
    ).count()

    ## Rename the 'count' column to 'total_complaints':
    result1 = result1.withColumnRenamed('count', 'Total_Complaints')

    ## Generate a result DataFrame of count of DISTINCT 'Company'
    ## by 'Product' and 'Year':
    result2 = complaints_df.groupBy(
        'Product',
        F.year('Date received').alias('Year')). \
        agg(F.countDistinct('Company'))

    ## Inner join two results by 'Product' and 'Year':
    result3 = result1.join(result2, on=['Product', 'Year'],
                           how='inner')

    ## Generate a result DataFrame of count of complaints
    ## against EACH 'Company' by 'Product' and 'Year':
    result4 = complaints_df.groupBy(
        'Product',
        F.year('Date received').alias('Year'),
        'Company') \
        .count()

    ## Generate a result DataFrame of the max count of complaints
    ## against ONE 'Company' by 'Product' and 'Year':
    result5 = result4.groupBy('Product', 'Year').max('count')
    result5 = result5.withColumnRenamed("max(count)", "Max_Complaints_One_Company")

    ## Inner join two DataFrames 'result3' & 'result5' by 'Product' and 'Year':
    result6 = result3.join(result5, on=['Product', 'Year'],
                           how='inner') \
        .orderBy('Product', 'Year', ascending=True)

    output = result6.withColumn('highest_percent_against_one_company',
                                result6['Max_Complaints_One_Company'] / result6['Total_Complaints'] * 100)

    ## Drop a column from the final output:
    output.drop('Max_Complaints_One_Company')

    ## Write the final output to a CSV file (without header):
    output.write.csv(sys.argv[2])
    ## Write the output DataFrame to a CSV file as the third argument in the command line.
    ## The first argument is the script, second is the input, and the third is the output.

    ...



## Add a “body” function, and create the SparkContext “sc” manually:
if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sc)
    ## Execute the main fuction:
    main(sc)


