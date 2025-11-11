# 🚀 PySpark

## 1.1 PySpark Overview

**PySpark** is the Python library for using **Apache Spark**, which is a big data processing framework.  
It allows you to write Python code to run on the Spark engine, which can process massive datasets in parallel.

 **Think of PySpark as:**

> “Python + Spark = PySpark”  
> Python’s simplicity + Spark’s power.

---

## 1.2 Role of PySpark in Big Data Processing

Imagine you have **1 TB of logs** from a website — too big for pandas or a single machine.

**Spark solves this by:**

- Breaking data into small chunks  
- Sending them to multiple computers (or CPU cores)  
- Running your logic in parallel  
- Combining results automatically  

 **PySpark = write once → run everywhere → fast + scalable**

---

## 1.3 Python API for Apache Spark

Apache Spark is written in **Scala**, but PySpark provides:

- A **Python API** that communicates with the **Spark engine (JVM)** underneath.

So your **Python code → PySpark → converts to Spark jobs in Scala → executes in the cluster.**

 **Behind the scenes:**

```
Your Python Code (PySpark)
        ↓
Py4J (gateway between Python ↔ Java)
        ↓
Spark Engine (runs on JVM)
```

---

# 💥 Spark Architecture

## 2.1 Spark Architecture (Core View)

Spark uses a **Master–Slave architecture**:

| **Component** | **Role** |
|----------------|----------|
| **Driver Program** | The “brain.” It sends tasks and collects results. |
| **Cluster Manager** | Allocates CPU/RAM to Spark jobs (YARN, Mesos, or Standalone). |
| **Worker Nodes** | The “workers” that run actual code. |
| **Executor** | Process running inside each worker; executes tasks. |
| **Task** | Smallest unit of execution — each part of your job. |

**Driver = Manager | Executors = Workers | Tasks = Work orders**

---

## 2.2 Integration with Spark Components

When you run a PySpark script:

1. Spark creates a **Driver Program** (in your Python shell or file).  
2. The Driver connects to the **Cluster Manager** (or your local system).  
3. The Cluster Manager assigns **Executors** on available cores.  
4. The Driver sends **Tasks** to Executors.  
5. Executors compute results and send them back to Driver.

 **Visualization:**

```
Driver → Cluster Manager → Executors → Tasks → Results
```

---

#  💥 Spark Components

| **Component** | **Description** |
|----------------|-----------------|
| **Driver Program** | Runs main() of your PySpark code. |
| **SparkContext** | The gateway between Python and Spark Engine. |
| **Executor** | Runs your transformations/actions. |
| **Task** | Unit of work executed on an Executor. |
| **Cluster Manager** | Controls and monitors resources (YARN/Mesos/Standalone). |

---

#  💥 SparkSession

## 4.1 What is SparkSession?

**SparkSession** is your entry point to PySpark — the object that lets you:

- Create **DataFrames**
- Run **SQL queries**
- Access **Spark Context**
- Configure **settings**

Without a SparkSession, PySpark code can’t run.

---

## 4.2 How to Create One

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder     .appName("DemoApp")     .getOrCreate()

print("Spark App Started:", spark)
```

`.builder` → starts configuration  
`.appName()` → gives your app a name  
`.getOrCreate()` → starts session if not already active  

 Always end with:

```python
spark.stop()
```

---

#  💥 DataFrame API

## 5.1 Overview

A **DataFrame** is a distributed collection of data organized into **named columns** — like a **SQL table** or a **pandas DataFrame**, but stored across a **cluster**.

### Example:

```python
data = [("Karan", 90), ("Ravi", 85)]
df = spark.createDataFrame(data, ["Name", "Score"])
df.show()
```

**Output:**

```
+-----+-----+
| Name|Score|
+-----+-----+
|Karan|   90|
| Ravi|   85|
+-----+-----+
```

---

## 5.2 PySpark vs Pandas

| **Feature** | **Pandas** | **PySpark** |
|--------------|-------------|-------------|
| **Runs on** | Single machine | Cluster (distributed) |
| **Memory** | Uses RAM on one PC | Spreads across many nodes |
| **Scale** | Small data (MBs/GBs) | Huge data (GBs–TBs) |
| **Execution** | Immediate | Lazy (only runs on action) |
| **Speed** | Slower for big data | Much faster for big data |

**For small data → use pandas**  
**For big data → use PySpark**

---

#  💥 Transformations and Actions

## 6.1 Key Idea

Spark works **lazily** — meaning it won’t execute transformations until an **action** is triggered.

- **Transformations** → Describe what to do (but don’t run).  
- **Actions** → Actually trigger execution.

---

## 6.2 Examples

| **Type** | **Example** | **Description** |
|-----------|-------------|-----------------|
| **Transformation** | `filter()`, `map()`, `select()`, `groupBy()` | Creates new dataset |
| **Action** | `show()`, `collect()`, `count()`, `first()` | Triggers actual execution |

### Example Code

```python
data = [("Karan", 90), ("Ravi", 85), ("Neha", 70)]
df = spark.createDataFrame(data, ["Name", "Score"])

filtered = df.filter(df.Score > 80)   # Transformation
filtered.show()                       # Action
```

**Output:**
```
+-----+-----+
| Name|Score|
+-----+-----+
|Karan|   90|
| Ravi|   85|
+-----+-----+
```

---

## ⚡ Summary Table

| **Concept** | **Description** |
|--------------|-----------------|
| **PySpark** | Python API for Apache Spark |
| **JVM** | Runs Spark Engine |
| **Py4J** | Bridge between Python and JVM |
| **Driver Program** | Controls Spark job and coordinates execution |
| **Executor** | Executes code in parallel on worker nodes |
| **Task** | Unit of work handled by executors |
| **DataFrame** | Distributed tabular data |
| **Transformation** | Lazy operation describing computation |
| **Action** | Triggers the actual computation |

---

# Final Note
**PySpark = Simplicity of Python + Power of Distributed Computing**

```
🔥 Write once → Run anywhere → Scale infinitely.
```
---

# 💥 PySpark RDDs (Resilient Distributed Datasets)

## 7.1 Overview of RDDs
**RDD (Resilient Distributed Dataset)** is the fundamental data structure in Spark.

- **Resilient** → Fault-tolerant; can recover lost data using lineage (transformation history).  
- **Distributed** → Data is split into partitions and processed across multiple nodes.  
- **Dataset** → Represents a collection of records.

**Creation Methods:**
```python
# Example 1: From a Python collection
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Example 2: From external data
rdd = sc.textFile("data.txt")

# Transformation + Action
rdd.map(lambda x: x * 2).collect()
```

---

## 7.2 Differences Between RDDs and DataFrames

| Feature | **RDD** | **DataFrame** |
|----------|----------|---------------|
| Type | Low-level data structure | High-level abstraction (like SQL table) |
| Data | Unstructured or semi-structured | Structured (rows + named columns) |
| Optimization | No automatic optimization | Optimized by Catalyst engine |
| Ease of Use | Functional (map, filter, reduce) | Declarative (select, where, groupBy) |
| Performance | Slower (no schema, no optimization) | Faster (Catalyst + Tungsten) |
| Use Case | Fine-grained control | SQL-style analytics |

---

# 💥 PySpark Data Structures

PySpark provides 3 major data abstractions:

1. **RDD** – Core distributed data structure.  
2. **DataFrame** – Structured abstraction built on RDDs with schema support.  
3. **Dataset** – (Available in Scala/Java only, not PySpark).

Hierarchy:
```
RDD → DataFrame → Dataset (typed)
```

---

# 💥 SparkContext

## 9.1 Role of SparkContext
- Acts as the **gateway** to the Spark cluster.
- Manages RDD creation, job scheduling, and cluster communication.
- Every PySpark app needs a SparkContext (usually created automatically by SparkSession).

## 9.2 Creating and Configuring SparkContext
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder     .appName("MyApp")     .master("local[*]")     .getOrCreate()

sc = spark.sparkContext
```

**Configuration Options:**
- `.appName("MyApp")` → Identifies your job.
- `.master("local[*]")` → Runs locally using all CPU cores.
- Additional configs like memory, executor cores, etc.

---

# 💥 PySpark DataFrames

## 10.1 Introduction
- A **DataFrame** is a distributed collection of rows with **named columns** (like a SQL table or Pandas DataFrame).  
- Built on top of RDDs, but with schema and optimization using **Catalyst Engine**.
- Can be created from:
  - RDDs
  - Python collections
  - External sources (CSV, JSON, Parquet)
  - Databases

**Example:**
```python
data = [("Karan", 24), ("Anu", 23)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()
```

---

## 10.2 DataFrame Operations

#### Basic Operations
```python
df.show()                 # Display data
df.printSchema()          # Display schema
df.select("name").show()  # Select specific column
df.filter(df.age > 23).show()  # Filter rows
df.groupBy("age").count().show()  # Group and count
```

#### Aggregations
```python
from pyspark.sql.functions import avg, max, min

df.select(avg("age")).show()
df.groupBy("age").agg(max("age"), min("age")).show()
```

#### Chained Operations
```python
df.filter(df.age > 23).select("name").show()
```

---

## ⚡ Summary

| Concept | Description |
|----------|-------------|
| **RDD** | Core distributed abstraction, resilient and parallel |
| **DataFrame** | High-level, structured, optimized abstraction |
| **SparkContext** | Bridge between PySpark app and Spark cluster |
| **SparkSession** | Unified entry point for creating DataFrames and SparkContext |
| **DataFrame Ops** | Easy SQL-like transformations and aggregations |

---

###  Quick Tip
- Use **RDDs** when you need low-level control.  
- Use **DataFrames** for performance, readability, and SQL-style analytics.  
- Always initialize Spark through `SparkSession` in PySpark.

---

# 💥 PySpark SQL Integration & Caching Guide

##  PySpark SQL Integration

###  What It Means
PySpark has a built-in SQL engine that allows you to run SQL queries directly on DataFrames. This enables you to seamlessly mix **SQL + Python** for analytics, transformations, and ETL workflows.

### Step-by-Step Example
```python
from pyspark.sql import SparkSession

# Step 1: Create Spark Session
spark = SparkSession.builder.appName("SQL_Integration").getOrCreate()

# Step 2: Create DataFrame
data = [("Karan", "IT", 50000), ("Ravi", "HR", 60000), ("Neha", "IT", 70000)]
df = spark.createDataFrame(data, ["Name", "Dept", "Salary"])

# Step 3: Register DataFrame as SQL table
df.createOrReplaceTempView("employees")

# Step 4: Run SQL query
result = spark.sql("SELECT Dept, AVG(Salary) AS Avg_Salary FROM employees GROUP BY Dept")

# Step 5: Display results
result.show()

spark.stop()
```

#### Output:
```
+----+-----------+
|Dept|Avg_Salary |
+----+-----------+
| IT | 60000.0   |
| HR | 60000.0   |
+----+-----------+
```

### What’s Happening
- `createOrReplaceTempView("employees")`: Registers the DataFrame as a temporary SQL table.
- You can now query it using `spark.sql("SELECT ...")`.

This demonstrates how Spark bridges **DataFrame APIs and SQL syntax**.

---

# 💥 Caching & Persisting

###  Why We Cache
Spark recomputes transformations every time an action (like `count()` or `show()`) is triggered.

To speed up repeated operations:
- Use **`cache()`** to store results in memory.
- Use **`persist()`** for more storage control (memory, disk, etc.).

### ⚙️ 2.1 Caching Example
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CachingDemo").getOrCreate()

data = [("Karan", "IT", 50000), ("Ravi", "HR", 60000), ("Neha", "IT", 70000)]
df = spark.createDataFrame(data, ["Name", "Dept", "Salary"])

# Cache DataFrame in memory
df.cache()

# Run some actions
print("First count:", df.count())
print("Second count:", df.count())  # Faster on 2nd run

spark.stop()
```

#### Without cache → Spark recomputes every time.  
With cache → Spark stores results in memory → no recomputation.

### 2.2 Persist Example
`persist()` gives you finer control:
```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)
```

#### Common Persistence Levels
| Storage Level | Description |
|----------------|--------------|
| MEMORY_ONLY | Default (fastest) — stores data in RAM |
| MEMORY_AND_DISK | Falls back to disk if not enough memory |
| DISK_ONLY | Stores only on disk |
| MEMORY_ONLY_SER | Stores serialized objects to save RAM |

---

### Quick Performance Comparison
| Operation | Without Cache | With Cache |
|------------|---------------|-------------|
| `.count()` | Recomputes DAG | Uses cached data |
| `.filter()` twice | Runs twice | Runs once (faster) |
| Repeated queries | Slow | Instant |

---

### ⚡ Summary Table
| Concept | Command | Purpose |
|----------|----------|----------|
| Register Temp SQL Table | `createOrReplaceTempView("name")` | Query DF using SQL |
| Run SQL | `spark.sql("SELECT * FROM name")` | Execute SQL on DataFrame |
| Cache DF | `df.cache()` | Store in memory for faster reuse |
| Persist DF | `df.persist(StorageLevel.MEMORY_AND_DISK)` | Store in memory + disk |
| Remove Cache | `df.unpersist()` | Free memory |

---

### In Short:
- **Spark SQL** = flexibility.
- **Caching** = performance .

---

# 💥 General DataFrame Functions

We'll use this same DataFrame for all examples 👇

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DataFrame_Functions").getOrCreate()

data = [("Karan", "IT", 90),
        ("Ravi", "HR", 85),
        ("Neha", "Finance", 70),
        ("Arjun", "IT", 95),
        ("Meena", "HR", 60)]

df = spark.createDataFrame(data, ["Name", "Dept", "Score"])
```

---

### 3.1 show()
Displays data in a tabular format (default 20 rows).

```python
df.show()
```
 **Output:**
```
+-----+-------+-----+
| Name|   Dept|Score|
+-----+-------+-----+
|Karan|     IT|   90|
| Ravi|     HR|   85|
| Neha|Finance|   70|
+-----+-------+-----+
```
 **Use:** Preview data.

---

### 3.2 collect()
Returns all rows as a list of Row objects (can be heavy on large data).

```python
df.collect()
```
 **Output:**
```
[Row(Name='Karan', Dept='IT', Score=90), Row(Name='Ravi', Dept='HR', Score=85), ...]
```
 **Use:** Bring data to Python (for small DataFrames only).

---

### 3.3 take(n)
Returns the first n rows as a list — safer than collect().

```python
df.take(2)
```
 **Output:**
```
[Row(Name='Karan', Dept='IT', Score=90), Row(Name='Ravi', Dept='HR', Score=85)]
```
 **Use:** Fetch sample data quickly.

---

### 3.4 printSchema()
Displays DataFrame column names and data types.

```python
df.printSchema()
```
 **Output:**
```
root
 |-- Name: string (nullable = true)
 |-- Dept: string (nullable = true)
 |-- Score: long (nullable = true)
```
**Use:** Check data types / structure.

---

### 3.5 count()
Returns total number of rows.

```python
df.count()
```
 **Output:** `5`

**Use:** Know dataset size.

---

### 3.6 select()
Choose one or more columns.

```python
df.select("Name", "Dept").show()
```
 **Output:**
```
+-----+-------+
| Name|   Dept|
+-----+-------+
|Karan|     IT|
| Ravi|     HR|
+-----+-------+
```
 **Use:** Column selection.

---

### 3.7 filter() / where()
Filter rows based on condition.

```python
df.filter(df.Score > 80).show()
# OR
df.where(col("Dept") == "IT").show()
```
 **Output:**
```
+-----+----+-----+
| Name|Dept|Score|
+-----+----+-----+
|Karan|  IT|   90|
|Arjun|  IT|   95|
+-----+----+-----+
```
 **Use:** Conditional filtering.

---

### 3.8 like()
Used inside filter/where for partial text matching (like SQL’s LIKE).

```python
df.filter(df.Name.like("K%")).show()
```
 **Output:**
```
+-----+----+-----+
| Name|Dept|Score|
+-----+----+-----+
|Karan|  IT|   90|
+-----+----+-----+
```
 **Use:** Pattern filtering.

---

### 3.9 sort()
Sort DataFrame rows by one or more columns.

```python
df.sort(df.Score.desc()).show()
```
 **Output:**
```
+-----+----+-----+
| Name|Dept|Score|
+-----+----+-----+
|Arjun|  IT|   95|
|Karan|  IT|   90|
| Ravi|  HR|   85|
| Neha|Finance|70|
|Meena|  HR|   60|
+-----+----+-----+
```
 **Use:** Sorting rows.

---

### 3.10 describe()
Returns statistical summary (like pandas describe()).

```python
df.describe().show()
```
 **Output:**
```
+-------+-----+-------+------------------+
|summary| Name|   Dept|             Score|
+-------+-----+-------+------------------+
|  count|    5|      5|                 5|
|   mean| null|   null|              80.0|
| stddev| null|   null|14.142135623730951|
|    min|Arjun|Finance|                60|
|    max| Ravi|     IT|                95|
+-------+-----+-------+------------------+
```
**Use:** Get stats like mean, stddev, min, max.

---

### 3.11 columns
Returns list of column names.

```python
df.columns
```
 **Output:**
```
['Name', 'Dept', 'Score']
```
 **Use:** Get all column names programmatically.

---

## ⚡ Summary Table
| Function | Use | Returns |
|-----------|-----|----------|
| `.show()` | View first 20 rows | Prints to console |
| `.collect()` | Get all rows as list | List[Row] |
| `.take(n)` | Get n rows | List[Row] |
| `.printSchema()` | Show structure | None |
| `.count()` | Row count | int |
| `.select()` | Choose columns | DataFrame |
| `.filter()` / `.where()` | Filter rows | DataFrame |
| `.like()` | Pattern match | DataFrame |
| `.sort()` | Sort rows | DataFrame |
| `.describe()` | Summary stats | DataFrame |
| `.columns` | List of columns | List[str] |

---

### Quick Memory Trick
#####  “show what you see, collect what you need, printSchema before you proceed.”

---

# 💥 PySpark String Functions (1–16)

## Base DataFrame
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("String_Functions").getOrCreate()

data = [("  karan  ", "IT-dept", "karan@company.com"),
        ("ravi", "HR-dept", "ravi@company.com"),
        ("neha", "Finance-dept", "neha@company.com")]

df = spark.createDataFrame(data, ["Name", "Department", "Email"])
df.show(truncate=False)
```

**Output:**
```
+---------+-------------+-------------------+
|Name     |Department   |Email              |
+---------+-------------+-------------------+
|  karan  |IT-dept      |karan@company.com  |
|ravi     |HR-dept      |ravi@company.com   |
|neha     |Finance-dept |neha@company.com   |
+---------+-------------+-------------------+
```

---

### 1.1 `upper()`
Converts text to uppercase.
```python
df.select(upper(col("Name")).alias("Upper_Name")).show()
```
**Output:** `KARAN`, `RAVI`, `NEHA`

---

### 1.2 `trim()`
Removes spaces from both ends.
```python
df.select(trim(col("Name")).alias("Trimmed")).show()
```
**Output:** `karan`, `ravi`, `neha`

---

### 1.3 `ltrim()`
Removes leading (left) spaces.
```python
df.select(ltrim(col("Name")).alias("Left_Trimmed")).show()
```

---

### 1.4 `rtrim()`
Removes trailing (right) spaces.
```python
df.select(rtrim(col("Name")).alias("Right_Trimmed")).show()
```

---

### 1.5 `substring_index()`
Splits a string by a delimiter and returns part before or after it.
```python
df.select(substring_index(col("Email"), "@", 1).alias("User_Name")).show()
```
 **Output:** `karan`, `ravi`, `neha`
 **Note:** `substring_index(col, '@', 1)` returns part before first `@`.

---

### 1.6 `substring()`
Extracts substring from position (1-based index).
```python
df.select(substring(col("Department"), 1, 2).alias("Dept_Code")).show()
```
 **Output:** `IT`, `HR`, `Fi`

---

### 1.7 `split()`
Splits a string into an array using a delimiter.
```python
df.select(split(col("Department"), "-").alias("Split_Dept")).show(truncate=False)
```
 **Output:** `[['IT', 'dept'], ['HR', 'dept'], ['Finance', 'dept']]`

---

### 1.8 `repeat()`
Repeats a string n times.
```python
df.select(repeat(col("Name"), 2).alias("Repeated")).show()
```
 **Output:** `karankaran`, `raviravi`, `nehaneha`

---

### 1.9 `rpad()`
Pads string on right to given length with a pattern.
```python
df.select(rpad(col("Name"), 10, "*").alias("Right_Padded")).show()
```
 **Output:** `karan*****`

---

### 1.10 `lpad()`
Pads string on left to given length.
```python
df.select(lpad(col("Name"), 10, "#").alias("Left_Padded")).show()
```
 **Output:** `#####karan`

---

### 1.11 `regex_replace()`
Replaces part of a string using regex.
```python
df.select(regex_replace(col("Department"), "-dept", "").alias("Clean_Dept")).show()
```
**Output:** `IT`, `HR`, `Finance`

---

### 1.12 `lower()`
Converts text to lowercase.
```python
df.select(lower(col("Department")).alias("Lower_Dept")).show()
```

---

### 1.13 `regex_extract()`
Extracts matching substring from a regex pattern.
```python
df.select(regex_extract(col("Email"), r'@(\\w+)', 1).alias("Domain")).show()
```
 **Output:** `company`
 **Explanation:** Extracts text between `@` and `.`

---

### 1.14 `length()`
Returns length of a string.
```python
df.select(length(col("Name")).alias("Length")).show()
```

---

### 1.15 `instr()`
Finds position (index) of substring.
```python
df.select(instr(col("Email"), "@").alias("At_Index")).show()
```
 **Output:** `6`

---

### 1.16 `initcap()`
Converts text to title case (first letter uppercase).
```python
df.select(initcap(col("Name")).alias("Title_Case")).show()
```
**Output:** `Karan`, `Ravi`, `Neha`

---

## ⚡ Summary Table
| Function | Purpose | Example |
|-----------|----------|----------|
| `upper()` | To uppercase | `upper(col("Name"))` |
| `trim()` | Remove spaces | `trim(col("Name"))` |
| `ltrim()` | Remove left spaces | `ltrim(col("Name"))` |
| `rtrim()` | Remove right spaces | `rtrim(col("Name"))` |
| `substring_index()` | Split & take part | `substring_index(col("Email"), "@", 1)` |
| `substring()` | Take substring | `substring(col("Dept"), 1, 3)` |
| `split()` | Split string into array | `split(col("Dept"), "-")` |
| `repeat()` | Repeat text | `repeat(col("Name"), 2)` |
| `rpad()` | Pad right side | `rpad(col("Name"), 10, "*")` |
| `lpad()` | Pad left side | `lpad(col("Name"), 10, "#")` |
| `regex_replace()` | Replace pattern | `regex_replace(col("Dept"), "-dept", "")` |
| `lower()` | Lowercase | `lower(col("Dept"))` |
| `regex_extract()` | Extract regex pattern | `regex_extract(col("Email"), "@(\\w+)", 1)` |
| `length()` | String length | `length(col("Name"))` |
| `instr()` | Find substring position | `instr(col("Email"), "@")` |
| `initcap()` | Title case | `initcap(col("Name"))` |

---

###  Quick Trick to Remember
 **trim → clean, split → break, regex → pattern, pad → shape, initcap → polish**

 ---

 # 💥 PySpark Numeric Functions

## Base DataFrame
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Numeric_Functions").getOrCreate()

data = [("Karan", 50000.567),
        ("Ravi", 60000.345),
        ("Neha", 70000.789),
        ("Arjun", 45000.111),
        ("Meena", 80000.955)]

df = spark.createDataFrame(data, ["Name", "Salary"])
df.show()
```

 **Output:**
```
+------+--------+
|  Name|  Salary|
+------+--------+
| Karan|50000.57|
|  Ravi|60000.35|
|  Neha|70000.79|
| Arjun|45000.11|
| Meena|80000.96|
+------+--------+
```

---

### 1.1 SUM()
Adds all numeric values together.
```python
df.select(sum("Salary").alias("Total_Salary")).show()
```
 **Output:**
```
+------------+
|Total_Salary|
+------------+
| 305002.766 |
+------------+
```
 **Use:** Get total or aggregate sum.

---

### 1.2 AVG()
Calculates average value.
```python
df.select(avg("Salary").alias("Average_Salary")).show()
```
 **Output:**
```
+--------------+
|Average_Salary|
+--------------+
|       61000.55|
+--------------+
```
 **Use:** Get mean or average.

---

### 1.3 MIN()
Finds the smallest number.
```python
df.select(min("Salary").alias("Min_Salary")).show()
```
 **Output:** `45000.11`  
 **Use:** Find minimum value.

---

### 1.4 MAX()
Finds the largest number.
```python
df.select(max("Salary").alias("Max_Salary")).show()
```
 **Output:** `80000.96`  
 **Use:** Find maximum value.

---

### 1.5 ROUND()
Rounds numeric value to given decimal places.
```python
df.select(round(col("Salary"), 1).alias("Rounded_Salary")).show()
```
 **Output:**
```
+--------------+
|Rounded_Salary|
+--------------+
|       50000.6|
|       60000.3|
|       70000.8|
|       45000.1|
|       80100.0|
+--------------+
```
**Use:** Round numbers.

---

### 1.6 ABS()
Returns the absolute (positive) value.
```python
df2 = spark.createDataFrame([("Karan", -5000), ("Ravi", 3000)], ["Name", "Change"])
df2.select(col("Name"), abs(col("Change")).alias("Absolute_Value")).show()
```
 **Output:**
```
+-----+--------------+
| Name|Absolute_Value|
+-----+--------------+
|Karan|          5000|
| Ravi|          3000|
+-----+--------------+
```
 **Use:** Convert negatives to positives.

---

## ⚡ Summary Table
| Function | Purpose | Example |
|-----------|----------|----------|
| `sum()` | Total | `sum("Salary")` |
| `avg()` | Mean | `avg("Salary")` |
| `min()` | Lowest | `min("Salary")` |
| `max()` | Highest | `max("Salary")` |
| `round()` | Round to decimals | `round(col("Salary"), 2)` |
| `abs()` | Absolute value | `abs(col("Change"))` |

---

###  Quick Trick to Remember
 **“Sum for total, Avg for level, Min/Max for range, Round for beauty, Abs for balance.”**

---

# 💥 Date and Time Functions

We'll use this base DataFrame 👇
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Date_Time_Functions").getOrCreate()

data = [("Karan", "2025-01-01"), ("Ravi", "2024-12-25"), ("Neha", "2023-06-15")]
df = spark.createDataFrame(data, ["Name", "Joining_Date"])
df.show()
```
**Output:**
```
+-----+------------+
|Name |Joining_Date|
+-----+------------+
|Karan|2025-01-01  |
|Ravi |2024-12-25  |
|Neha |2023-06-15  |
+-----+------------+
```

### 1.1 `CURRENT_DATE()`
Returns the current system date.
```python
df.select(current_date().alias("Today")).show()
```

### 1.2 `CURRENT_TIMESTAMP()`
Returns current system date and time.
```python
df.select(current_timestamp().alias("Current_Time")).show()
```

### 1.3 `DATE_ADD()`
Adds a number of days to a date.
```python
df.select(date_add(col("Joining_Date"), 10).alias("After_10_Days")).show()
```

### 1.4 `DATEDIFF()`
Calculates difference between two dates in days.
```python
df.select(datediff(current_date(), col("Joining_Date")).alias("Days_Since_Join")).show()
```

### 1.5 `YEAR()`
Extracts year from date.
```python
df.select(year(col("Joining_Date")).alias("Year")).show()
```

### 1.6 `MONTH()`
Extracts month number from date.
```python
df.select(month(col("Joining_Date")).alias("Month")).show()
```

### 1.7 `DAY()`
Extracts day of month.
```python
df.select(dayofmonth(col("Joining_Date")).alias("Day")).show()
```

### 1.8 `TO_DATE()`
Converts string to date type.
```python
df.select(to_date(col("Joining_Date"), "yyyy-MM-dd").alias("To_Date")).printSchema()
```

### 1.9 `DATE_FORMAT()`
Formats date into custom string format.
```python
df.select(date_format(col("Joining_Date"), "MMM dd, yyyy").alias("Formatted_Date")).show()
```
---

## ⚡ Summary Table

| Category | Function | Purpose |
|-----------|-----------|----------|
| **Date & Time** | `CURRENT_DATE()` | Get current date |
| | `DATEDIFF()` | Days between two dates |
| | `DATE_ADD()` | Add days |
| | `DATE_FORMAT()` | Format date string |

---

# 💥  Aggregate Functions

We'll use 👇
```python
data = [("IT", 50000), ("HR", 60000), ("Finance", 70000), ("IT", 55000), ("HR", 65000)]
df = spark.createDataFrame(data, ["Dept", "Salary"])
```

### 1.1 `mean()` / 2.2 `avg()`
Calculate average value.
```python
df.groupBy("Dept").agg(mean("Salary").alias("Mean_Salary")).show()
```

### 1.3 `collect_list()`
Returns list of all values in group (duplicates included).
```python
df.groupBy("Dept").agg(collect_list("Salary").alias("Salaries")).show(truncate=False)
```

### 1.4 `collect_set()`
Returns unique values as a list.
```python
df.groupBy("Dept").agg(collect_set("Salary").alias("Unique_Salaries")).show(truncate=False)
```

### 1.5 `countDistinct()`
Counts distinct values.
```python
df.select(countDistinct("Salary").alias("Unique_Salary_Count")).show()
```

### 1.6 `count()`
Counts total rows or values.
```python
df.groupBy("Dept").count().show()
```

### 1.7 `first()` / 1.8 `last()`
Return first or last value in a column.
```python
df.agg(first("Salary").alias("First_Salary"), last("Salary").alias("Last_Salary")).show()
```

### 1.9 `max()` / 1.10 `min()`
Get highest or lowest value.
```python
df.select(max("Salary").alias("Max_Salary"), min("Salary").alias("Min_Salary")).show()
```

### 1.11 `sum()`
Returns total of numeric column.
```python
df.groupBy("Dept").agg(sum("Salary").alias("Total_Salary")).show()
```
---

## ⚡ Summary Table

| Category | Function | Purpose |
|-----------|-----------|----------|
| **Aggregate** | `sum()` / `avg()` / `mean()` | Total or average |
| | `collect_list()` / `collect_set()` | List / Unique List |
| | `count()` / `countDistinct()` | Count rows / unique rows |

---

# 💥 Joins in PySpark

We'll use two DataFrames 👇
```python
emp = [(1, "Karan", 1), (2, "Ravi", 2), (3, "Neha", 3), (4, "Meena", 2)]
dept = [(1, "IT"), (2, "HR"), (3, "Finance")]
emp_df = spark.createDataFrame(emp, ["Emp_ID", "Name", "Dept_ID"])
dept_df = spark.createDataFrame(dept, ["Dept_ID", "Dept_Name"])
```

### 1.1 Inner Join
Returns matching rows from both tables.
```python
emp_df.join(dept_df, "Dept_ID", "inner").show()
```

### 1.2 Cross Join
Cartesian product — all combinations.
```python
emp_df.crossJoin(dept_df).show()
```

### 1.3 Outer Join (Full Outer)
Returns all rows from both DataFrames, null if no match.
```python
emp_df.join(dept_df, "Dept_ID", "outer").show()
```

### 1.4 Left Join
All rows from left DataFrame, matched data from right.
```python
emp_df.join(dept_df, "Dept_ID", "left").show()
```

### 1.5 Right Join
All rows from right DataFrame, matched data from left.
```python
emp_df.join(dept_df, "Dept_ID", "right").show()
```

### 1.6 Left Semi Join
Keeps only rows from left DataFrame that have a match in right.
```python
emp_df.join(dept_df, "Dept_ID", "left_semi").show()
```

### 1.7 Left Anti Join
Keeps rows from left DataFrame that **don’t** have a match in right.
```python
emp_df.join(dept_df, "Dept_ID", "left_anti").show()
```

---

## ⚡ Summary Table

| Category | Function | Purpose |
|-----------|-----------|----------|
| **Joins** | `inner`, `left`, `right`, `outer` | Standard joins |
| | `left_semi`, `left_anti` | Filtered joins |

---

### Quick Recap
**Date = Time Intelligence | Aggregate = Insights | Join = Relationships**


# 💥 Mathematical Functions

We'll use this base DataFrame 👇
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Math_Functions").getOrCreate()

data = [(1, -4.5), (2, 9.3), (3, 2.7), (4, 0.5)]
df = spark.createDataFrame(data, ["ID", "Value"])
df.show()
```
**Output:**
```
+---+-----+
|ID |Value|
+---+-----+
| 1 |-4.5 |
| 2 | 9.3 |
| 3 | 2.7 |
| 4 | 0.5 |
+---+-----+
```

### 1.1 `ABS()`
Returns absolute value.
```python
df.select(col("ID"), abs(col("Value")).alias("Absolute")).show()
```

### 1.2 `CEIL()` / `CEILING()`
Rounds value **up** to nearest integer.
```python
df.select(col("Value"), ceil(col("Value")).alias("Ceil_Value")).show()
```

### 1.3 `FLOOR()`
Rounds value **down** to nearest integer.
```python
df.select(col("Value"), floor(col("Value")).alias("Floor_Value")).show()
```

### 1.4 `EXP()`
Returns e^x (exponential value).
```python
df.select(col("Value"), exp(col("Value")).alias("Exponential")).show()
```

### 1.5 `LOG()`
Returns natural logarithm (base e).
```python
df.select(col("Value"), log(col("Value")).alias("Log_Value")).show()
```

### 1.6 `POWER()`
Raises value to a given power.
```python
df.select(col("Value"), pow(col("Value"), 2).alias("Power_2")).show()
```

### 1.7 `SQRT()`
Returns square root.
```python
df.select(col("Value"), sqrt(col("Value")).alias("Square_Root")).show()
```
---
## ⚡ Summary Table

| Category | Function | Purpose |
|-----------|-----------|----------|
| **Mathematical** | `ABS()` | Absolute value |
| | `CEIL()` / `FLOOR()` | Round up/down |
| | `EXP()` / `LOG()` | Exponential / Natural Log |
| | `POWER()` / `SQRT()` | Power & Square root |

---

# 💥 Conversion Functions

### 1.1 `CAST()`
Converts one data type to another.

```python
from pyspark.sql.types import IntegerType

data = [("100",), ("250",), ("400",)]
df2 = spark.createDataFrame(data, ["Amount"])
df2.select(col("Amount"), col("Amount").cast(IntegerType()).alias("Amount_Int")).printSchema()
```
## ⚡ Summary Table

| Category | Function | Purpose |
|-----------|-----------|----------|
| **Conversion** | `CAST()` | Change data type |

---

 **Use:** Convert string → integer, float → string, etc.

---

# 💥 Window Functions

We'll use this base DataFrame 👇
```python
from pyspark.sql.window import Window

data = [("Karan", "IT", 5000),
        ("Ravi", "IT", 6000),
        ("Neha", "HR", 5500),
        ("Arjun", "HR", 6500),
        ("Meena", "Finance", 7000)]

df = spark.createDataFrame(data, ["Name", "Dept", "Salary"])
windowSpec = Window.partitionBy("Dept").orderBy(col("Salary").desc())
df.show()
```

**Output:**
```
+------+-------+------+
| Name | Dept  |Salary|
+------+-------+------+
|Karan | IT    | 5000 |
|Ravi  | IT    | 6000 |
|Neha  | HR    | 5500 |
|Arjun | HR    | 6500 |
|Meena |Finance| 7000 |
+------+-------+------+
```

### 1.1 `ROW_NUMBER()`
Gives unique sequential number per partition (no ties).
```python
df.withColumn("Row_Num", row_number().over(windowSpec)).show()
```

### 1.2 `RANK()`
Gives ranking within partition — **ties get same rank, gaps follow**.
```python
df.withColumn("Rank", rank().over(windowSpec)).show()
```

### 1.3 `DENSE_RANK()`
Ranks without gaps (even if values tie).
```python
df.withColumn("Dense_Rank", dense_rank().over(windowSpec)).show()
```

### 1.4 `LEAD()`
Access next row’s value within same partition.
```python
df.withColumn("Next_Salary", lead("Salary", 1).over(windowSpec)).show()
```

### 1.5 `LAG()`
Access previous row’s value within same partition.
```python
df.withColumn("Prev_Salary", lag("Salary", 1).over(windowSpec)).show()
```

---

## ⚡ Summary Table

| Category | Function | Purpose |
|-----------|-----------|----------|
| **Window** | `ROW_NUMBER()` | Sequential index per partition |
| | `RANK()` / `DENSE_RANK()` | Rank with/without gaps |
| | `LEAD()` / `LAG()` | Next / Previous row access |

---

### Quick Recap
 **Math = Calculate | Conversion = Change Type | Window = Rank & Compare**


