# ⚡PySpark

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

#  Summary Table

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

##  Summary

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

### Summary Table
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

##  Summary Table
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

