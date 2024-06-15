from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
import os
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@task
def extraction():
    spark = SparkSession.builder.getOrCreate()
    
    employees = spark.read.option("header", True).csv("./data/employees_202406121535.csv")
    dept_emp = spark.read.option("header", True).csv("./data/dept_emp_202406121532.csv")
    departments = spark.read.option("header", True).csv("./data/departments_202406121534.csv")
    salaries = spark.read.option("header", True).csv("./data/salaries_202406121535.csv")
    
    employees_list = employees.toPandas().to_dict(orient='records')
    dept_emp_list = dept_emp.toPandas().to_dict(orient='records')
    departments_list = departments.toPandas().to_dict(orient='records')
    salaries_list = salaries.toPandas().to_dict(orient='records')
    
    spark.stop()
    
    return {
        'employees': employees_list,
        'dept_emp': dept_emp_list,
        'departments': departments_list,
        'salaries': salaries_list
    }


@task
def transform(dfs):
    spark = SparkSession.builder.getOrCreate()
    
    employees = spark.createDataFrame(dfs['employees'])
    dept_emp = spark.createDataFrame(dfs['dept_emp'])
    departments = spark.createDataFrame(dfs['departments'])
    salaries = spark.createDataFrame(dfs['salaries'])
    
    mean_salaries = salaries.groupby('emp_no').agg({'salary': 'mean'})
    sum_salaries = salaries.groupby('emp_no').agg({'salary': 'sum'})

    merge = employees.join(dept_emp, 'emp_no').select('emp_no', 'gender', 'dept_no')
    merge = merge.join(departments, 'dept_no').select('emp_no', 'gender', 'dept_name')
    merge = merge.join(mean_salaries, 'emp_no').select('emp_no', 'gender', 'dept_name', 'avg(salary)')
    merge = merge.join(sum_salaries, 'emp_no').select('gender', 'dept_name', 'avg(salary)', 'sum(salary)')
    merge = merge.withColumnRenamed('avg(salary)', 'avg_salary')
    merge = merge.withColumnRenamed('sum(salary)', 'sum_salary')
    
    merge = merge.toPandas().to_dict(orient='records')

    spark.stop()
    
    return merge


@task
def load(df, path):
    spark = SparkSession.builder.getOrCreate()
    
    df = spark.createDataFrame(df)
    df.repartition(1).write.options(header='True', delimiter=',').csv(path)
    
    dir = os.listdir(path)
    if not os.path.exists("dest_data"): os.mkdir("dest_data")
    os.chdir(path)
    for file in dir:
        if (file.split('.')[-1] == 'csv'):
            new_path = "../dest_data/" + path + ".csv"
            os.rename(file, new_path)
        else:
            os.remove(file)
    os.chdir("../")
    os.rmdir(path)
    
    spark.stop()


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

@dag(dag_id='society_etl', default_args=default_args, schedule_interval='@daily', catchup=False)
def society_etl():
    dfs = extraction()
    merge = transform(dfs)

    trigger = TriggerDagRunOperator(
        task_id="run_society_analysis_dag",
        trigger_dag_id="society_analysis",
    )
    
    load(merge, "society") >> trigger

etl = society_etl()