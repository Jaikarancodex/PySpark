# ⚡PySpark

##  PySpark Overview

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

