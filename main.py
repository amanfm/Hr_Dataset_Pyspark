from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import desc
import pandas as pd

spark = SparkSession.builder.appName('hr_dataset').getOrCreate()

employee = spark.read.csv('/home/aman/Downloads/employee.csv', header="true")
# employee.show(4)
employee.write.csv("hdfs://usr/local/hadoop_store/hdfs/datanode/example.csv")


jobs = spark.read.csv('/home/aman/Downloads/jobs.csv', header="true")
# jobs.show(4)

locations = spark.read.csv('/home/aman/Downloads/locations.csv', header="true")

history = spark.read.csv('/home/aman/Downloads/history.csv', header="true")

department = spark.read.csv('/home/aman/Downloads/department.csv', header="true")

print("showing only nan values")
employee.select([count(when(isnan(c), c)).alias(c) for c in employee.columns]).show()

print("showing nan and null values both")
employee.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in employee.columns]).show()

print("showing nan and null values both locations file")
locations.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in locations.columns]).show()

employee_m = employee.select("first_name", "last_name", "salary", "department_id").filter(
    employee.first_name.like("%m"))
# employee_m.show(5)

employee.select("first_name", "last_name", "salary", "department_id") \
    .filter(employee.first_name.like("%d") | employee.first_name.like("%n") | employee.first_name.like("%s")) \
    .sort(desc("department_id")) \
    .show(5)

employee.select("employ_id", "first_name", "last_name", "department_id") \
    .filter(employee.department_id.isin(70, 90)) \
    .show(5)

employee.select("first_name", "last_name", "salary", "department_id") \
    .filter(employee.first_name.like("__n%")) \
    .show(4)

employee.select("first_name", "job_id", "salary", "department_id") \
    .filter(~employee.department_id.isin(50, 30, 80)) \
    .show(5)

history.select("employee_id") \
    .groupBy(history.employee_id) \
    .count().filter("count>=2") \
    .show()

employee.select("salary") \
    .agg(f.min("salary").alias("min_salary"), \
         f.max("salary").alias("max_salary"), \
         f.mean("salary").alias("mean_salary")) \
    .show()

employee.select("first_name", "last_name", "hire_date") \
    .filter(employee.job_id.isin('SA_REP', 'SA_MAN')) \
    .show(5)

employee.select("first_name", "last_name", "department_id") \
    .filter(employee.last_name == "McEwen") \
    .show()

# locations.show()
# print("\n\n   State Province(Not null / Null Series")
# print(locations.state_province.isNull())

pd_locations = pd.read_csv('/home/aman/Downloads/locations.csv')
print(pd_locations.state_province)
print(pd_locations.state_province.notnull())

print("department average salary join")
emp_dep = employee.join(department, employee.department_id == department.department_id, "inner")

avg_department_name = emp_dep.select("department_name", "salary") \
    .groupBy(emp_dep.department_name) \
    .agg({"salary": "avg"}) \
    .show()

print("count department located in seatle city")
dep_location_join = department.join(locations, department.location_id == locations.location_id, "inner")
dep_location_join.show()

print("seattle count")
dep_seattle = dep_location_join.filter(dep_location_join.city == 'Seattle') \
    .select(count("city")) \
    .show()

print("joins on employee and jobs")
empl_jobs_join = employee.join(jobs, employee.job_id == jobs.job_id, "inner")
empl_jobs_join.show(5)

job_finance_avg = empl_jobs_join.filter(empl_jobs_join.job_title == 'Finance Manager') \
    .groupBy("job_title") \
    .agg({"salary": "avg"}) \
    .show()

epml_mark_sales = empl_jobs_join.filter((empl_jobs_join.job_title == 'Sales Representative') | (empl_jobs_join.job_title == 'Marketing Representative')) \
   .groupBy("job_title") \
   .count()\
    .show()