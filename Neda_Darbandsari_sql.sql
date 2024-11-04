-- Databricks notebook source
-- ˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚ TASK 1 ˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚-- 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql import Row
-- MAGIC from pyspark.sql.functions import col
-- MAGIC import matplotlib.pyplot as plt

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC FilePath="/FileStore/tables/clinicaltrial_2023"
-- MAGIC dbutils.fs.ls(FilePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC RDD = sc.textFile(FilePath)
-- MAGIC RDD.take(3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Delimiter="\t"
-- MAGIC RDD_Cleaned = RDD.map(lambda s: s.replace(',,', '').replace('"', '').split(Delimiter))
-- MAGIC RDD_Cleaned.take(2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def set_length(row, length):
-- MAGIC     return row + ([None] * (length - len(row)))
-- MAGIC
-- MAGIC RDD_Final = RDD_Cleaned.map(lambda row: set_length(row, 14))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Schema = StructType([
-- MAGIC     StructField("Id", StringType()),
-- MAGIC     StructField("StudyTitle", StringType()),
-- MAGIC     StructField("Acronym", StringType()),
-- MAGIC     StructField("Status", StringType()),
-- MAGIC     StructField("Conditions", StringType()),
-- MAGIC     StructField("Interventions", StringType()),
-- MAGIC     StructField("Sponsor", StringType()),
-- MAGIC     StructField("Collaborators", StringType()),
-- MAGIC     StructField("Enrollment", StringType()),
-- MAGIC     StructField("FunderType", StringType()),
-- MAGIC     StructField("Type", StringType()),
-- MAGIC     StructField("StudyDesign", StringType()),
-- MAGIC     StructField("Start", StringType()),
-- MAGIC     StructField("Completion", StringType())
-- MAGIC ])
-- MAGIC
-- MAGIC DF = spark.createDataFrame(RDD_Final, Schema)
-- MAGIC Header = DF.first()
-- MAGIC RDD_New = DF.rdd.filter(lambda line: line != Header)
-- MAGIC DF = spark.createDataFrame(RDD_New, DF.schema)
-- MAGIC
-- MAGIC DF.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF. createOrReplaceTempView ("ClinicalTrial")

-- COMMAND ----------

SELECT * FROM ClinicalTrial  LIMIT 10

-- COMMAND ----------

--Question Number 1

SELECT COUNT(DISTINCT `StudyTitle`) AS Study_Count_Distinct
FROM clinicaltrial;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

--Question Number 2

SELECT
    CASE
        WHEN Type = '' THEN 'Blank Values'
        ELSE Type
    END AS Types,
    COUNT(*) AS Count
FROM Clinicaltrial
WHERE Type IS NOT NULL AND Type != 'Type'
GROUP BY Types
ORDER BY Count DESC;

-- COMMAND ----------

--Question Number 3

SELECT Condition, COUNT(*) AS Frequency
FROM (
    SELECT EXPLODE(SPLIT(Conditions, '\\|')) AS Condition
    FROM ClinicalTrial
    WHERE Conditions IS NOT NULL
)
GROUP BY condition
ORDER BY Frequency DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("/FileStore/tables/pharma", header=True, inferSchema=True)
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("Pharma")

-- COMMAND ----------

SELECT * FROM Pharma LIMIT 10

-- COMMAND ----------

SELECT Sponsor, COUNT(Sponsor) from ClinicalTrial DESC GROUP BY Sponsor

-- COMMAND ----------

--Question Number 4

SELECT Sponsor, COUNT(*) AS SponsorCount
FROM ClinicalTrial
WHERE Sponsor NOT IN (SELECT Parent_company FROM Pharma)
GROUP BY Sponsor
ORDER BY SponsorCount DESC LIMIT 10;

-- COMMAND ----------

SELECT Completion, CAST(Completion AS DATE) AS CompletionDate
FROM ClinicalTrial;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/clinicaltrial", True)

-- COMMAND ----------

CREATE TABLE ClinicalTrial AS
SELECT * FROM ClinicalTrial;

DROP VIEW ClinicalTrial;

-- COMMAND ----------

ALTER TABLE ClinicalTrial
ADD COLUMN CompletedDate DATE;

-- COMMAND ----------

UPDATE ClinicalTrial
SET Completion = REPLACE(Completion, ',', '');
SET CompletedDate = CAST(Completion AS DATE);

-- COMMAND ----------

--Question Number 5

CREATE OR REPLACE TEMPORARY VIEW Completed AS
SELECT CAST(CASE 
            WHEN LENGTH(CAST(Completion AS STRING)) = 7 THEN CONCAT(CAST(Completion AS STRING), '-01')  
            ELSE Completion
        END AS DATE
    ) AS CompletionDate
FROM ClinicalTrial
WHERE CAST(CASE 
            WHEN LENGTH(CAST(Completion AS STRING)) = 7 THEN CONCAT(CAST(Completion AS STRING), '-01')  
            ELSE Completion
        END AS DATE
    ) BETWEEN CAST('2023-01-01' AS DATE) AND CAST('2023-12-31' AS DATE)
    AND Status = 'COMPLETED';


SELECT MONTH(CompletionDate) AS Month, COUNT(*) AS CompletedCount
FROM Completed
GROUP BY MONTH(CompletionDate)
ORDER BY Month;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC SQL_Completed = """
-- MAGIC WITH Completed AS (
-- MAGIC     SELECT CAST(CASE 
-- MAGIC                 WHEN LENGTH(CAST(Completion AS STRING)) = 7 THEN CONCAT(CAST(Completion AS STRING), '-01')  
-- MAGIC                 ELSE Completion
-- MAGIC             END AS DATE
-- MAGIC         ) AS CompletionDate
-- MAGIC     FROM ClinicalTrial
-- MAGIC     WHERE CAST(CASE 
-- MAGIC                 WHEN LENGTH(CAST(Completion AS STRING)) = 7 THEN CONCAT(CAST(Completion AS STRING), '-01')  
-- MAGIC                 ELSE Completion
-- MAGIC             END AS DATE
-- MAGIC         ) BETWEEN CAST('2023-01-01' AS DATE) AND CAST('2023-12-31' AS DATE)
-- MAGIC         AND Status = 'COMPLETED'
-- MAGIC )
-- MAGIC SELECT MONTH(CompletionDate) AS Month, COUNT(*) AS CompletedCount
-- MAGIC FROM Completed
-- MAGIC GROUP BY MONTH(CompletionDate)
-- MAGIC ORDER BY Month
-- MAGIC """
-- MAGIC
-- MAGIC DF_Completed = spark.sql(SQL_Completed)
-- MAGIC DF_Final = DF_Completed.toPandas()
-- MAGIC
-- MAGIC plt.figure(figsize=(15, 8))
-- MAGIC plt.bar(DF_Final['Month'], DF_Final['CompletedCount'], color='plum')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.title('Number Of Completed Studies For Each Month In 2023')
-- MAGIC plt.show()

-- COMMAND ----------

--˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚ EXTRA ANALYSIS ˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚•˚--

-- COMMAND ----------

--Study Status Trend: Counting the number of studies over time grouped by study status (recruiting, active, completed) to identify trends in the status of clinical trials over time.

SELECT 
    Status,
    COUNT(*) AS count,
    ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM 
    ClinicalTrial
GROUP BY 
    Status;

-- COMMAND ----------

--Funder Type Distribution: Analyzing the distribution of funder types (government, non-profit, industry) across all trials.

WITH FunderTypeDistribution AS (
    SELECT
        FunderType,
        COUNT(*) AS TrialCount
    FROM
        ClinicalTrial
    GROUP BY
        FunderType
)

SELECT
    *
FROM
    FunderTypeDistribution;

-- COMMAND ----------

--Study Duration Analysis: Calculating the duration of each study (in years) and analyze the distribution of study durations.

WITH StudyDuration AS (
    SELECT
        Start,
        Completion,
        DATEDIFF(Completion, Start) / 365 AS StudyDurationYears
    FROM
        ClinicalTrial
)
SELECT
    percentile_approx(StudyDurationYears, 0.25) AS Q1,
    percentile_approx(StudyDurationYears, 0.50) AS Median,
    percentile_approx(StudyDurationYears, 0.75) AS Q3,
    max(StudyDurationYears) AS Max,
    min(StudyDurationYears) AS Min,
    avg(StudyDurationYears) AS Mean,
    stddev(StudyDurationYears) AS StdDev
FROM
    StudyDuration;
