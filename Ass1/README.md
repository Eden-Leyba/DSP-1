# About the project
Eden Leyba ***REMOVED***
Noa Yaakov ***REMOVED***
## 1. Introduction

This project implements a distributed text-analysis system deployed on Amazon Web Services (AWS).
Each Local Application submits an input file containing multiple text‐file URLs together with their required analysis type (POS, Constituency, or Dependency).
Upon submission, the file is uploaded to Amazon S3 and a processing request is sent to the cloud.

A dedicated Manager EC2 instance orchestrates the distributed computation.
It retrieves the input file, decomposes it into individual analysis tasks, and launches Worker EC2 instances according to the workload and the parameter n (maximum number of files per worker).
Task distribution and result collection are performed through Amazon SQS queues.

Worker instances operate independently: each retrieves tasks from the queue, downloads the corresponding text file, performs the requested NLP analysis using the Stanford Parser, and uploads the processed output to S3.
The Manager aggregates all results belonging to a specific Local Application, constructs a single HTML summary file, stores it in S3, and notifies the originating Local Application via SQS.

This system was designed in accordance with the distributed-systems principles taught in class.

It supports scalable execution under heavy workloads, persistent and fault-tolerant behavior in the presence of node failures, and correct recovery from stalled or interrupted processing through SQS visibility timeouts and message re-delivery.
The architecture also ensures correct behavior when multiple Local Applications run concurrently, proper division of responsibilities between Manager and Workers, and careful consideration of when threading is appropriate within individual components.
All of which will be discussed in detail in the following sections.

## 2. High-Level Architecture

The system is composed of three logically independent components:

1. **Local Application** – runs on the client machine.
2. **Manager** – a single EC2 instance coordinating the distributed computation.
3. **Workers** – a dynamic number of EC2 instances performing text analysis.

Communication is performed exclusively through **Amazon SQS**, while all files (inputs and summaries) are stored in **Amazon S3**.  
The clients which are currently in the system are logged using **Amazon DynamoDB**. This also help ensure progress and failure control.

## 3.SQS Queue Protocols
We use four dedicated queues, each representing a clean communication channel:

### Locals Output
Locals (Many) -> Manager (One)  
Send a message once a local app is up and has an input file to process.   
The input file is uploaded to S3 in a bucket denoted *bucket* and key denoted *key*  
In addition, *n* is the number workers’ files ratio (max files per worker)  
Message format:   
If it's a job message:
"job" + "\n" + bucket + "\n" + key + "\n" + n  
If it's a termination message:
terminate

### Workers Tasks Incoming
Manager (One) -> Workers (Many)  
A worker pulls the first message on the queue  
New analysis task message from the manager to the workers  
Contains the URL of a specific text file and the required analysis  
Message format:  
local_id + requested_analysis + "\n" + url of input file to analyze + "\n"

### Workers Tasks Done
Workers (Many) -> Manager (one)  
When a worker finishes a file, it sends a done message to the manager
The message contains the S3 location of the analysis file the type of
analysis, and the URL of the input file  
Message Format:  
On Success:  
local_id + "\n" + input_file_to_analyze_url + "\n" + worker_bucket_name + "\n" + analyzed_file_key + "\n" + requested_analysis  
On Failure:  
local_id + "\n" + input_file_to_analyze_url + "\n" + "ERROR" + "\n" + "ERROR" + "\n" + requested_analysis + error_description

### Locals Input
Manager (One) -> Locals (Many)  
Done message from the manager to the application
Contains the local_id number (assigned by the Manager) and the location of the analysis summary file  
Message format:  
local_id + "\n" + summary_bucket + "\n" + summary_key

## 3. Component Responsibilities

### 3.1 Local Application

The Local Application is responsible for:

1. **Manager detection and bootstrap:**  
   It checks whether a Manager EC2 instance is active. If not, it launches one automatically.
   This process is repeated for every 60s: The client checks for an active manager, and if it does not find one, it starts one.


2. **Input file handling:**  
   The Local Application uploads the input file to a unique S3 bucket.

- A **job message** containing:
   - the S3 bucket name,
   - the key of the uploaded file,
   - the parameter `n` (max files per worker)  
     is sent to **LocalsOutput**.

3. **Awaiting results:**  
   The application waits on **LocalsInput** for a summary message identifying the S3 bucket and key of the final HTML file.  
   It then downloads this summary.

4. **Optional termination:**  
   If the client is run with the `"terminate"` flag, the Local Application sends a **termination message** to the Manager after receiving its own results.

The Local Application is **fully multi-client safe**: multiple clients can run simultaneously without interference.

### 3.2 Manager

The Manager is the **central orchestrator** of the assignment.  
It is responsible for scheduling the workers, scaling, fault-management, and final aggregation.

Its responsibilities are divided into two internal threads:



#### 3.2.1 Local Requests Listener (LocalsListenerRunnable)

This thread continuously listens to **LocalsOutput**.

For each incoming **job message**, it:

1. **Downloads the input file from S3**  
   Extracts the list of  tasks, containing analysis and url.

2. **Generates analysis tasks for Workers**  
   One SQS message per URL is posted to **Workers_Tasks_Incoming**, enabling parallelism by using thread pool.

3. **Computes required number of Worker instances**  
   Let `k` be currently active workers, and `m ` the required count.  
   The Manager starts up to `m − k` new EC2 Worker instances (bounded by `MAX_WORKER_INSTANCES` - given by the assignmet).

4. **Initializes DynamoDB tracking entry**  
   For each Local Application, an entry stores:
   - total number of URLs,
   - number processed so far,
   - S3 location of the summary file.

   This ensures **persistent state**, so Manager or Worker failures do not compromise execution.

- For **termination messages**, the Manager:
   - stops accepting new jobs via **LocalsOutput**.
   - waits for all Worker queues to empty,
   - triggers system-wide shutdown.



#### 3.2.2 Worker Results Listener (WorkerListenerRunnable)

This thread continuously monitors **Workers_Tasks_Done**.

For each completed analysis :

1. **Append result to HTML summary file in S3**, increasing the amount of url done.

2. **Update progress in DynamoDB**  
   When all URLs for a given Local Application are done, the Manager sends a **completion message** via **LocalsInput** to the corespondin local id.

3. **Final termination detection**  
   Once all queues are empty and termination mode is active, the Manager safely shuts down all Worker instances.

The Manager thus behaves as a **concurrency-aware scheduler**, supporting multiple Local Applications simultaneously using a thread pool.



### 3.3 Workers

Each Worker EC2 instance repeatedly performs the following cycle:

1. **Fetch exactly one task** from **Workers_Tasks_Incoming**.

2. **Start visibility-extension thread**:  
   This thread periodically renews the SQS visibility timeout, ensuring no other Worker receives the same task while processing long files.

3. **Download the input text file** using requested Linux utilities (`wget`).  
   If this operation failed, the worker prepares an error message and sends it to the manager, Then continues to the next task

4. **Perform the required analysis** using the Stanford Parser.  
   Workers are designed to be:

5. **Upload result** to an S3 bucket dedicated to the Worker instance.

6. **Send a completion message** to **Workers_Tasks_Done**.

7. **Delete the task message** once completed.

- If a Worker crashes during processing:
   - the message eventually becomes visible again,
   - another Worker will process it.  
     This satisfies the requirement for **fault-recovery and persistence**.

### 3.4 Common Utils Module

**3.4.1 Amazon Utils**  

This class helps handle all requests sent to amazon, so the actual code of the other modules is clean using Separation Of Concerns Principle.  
This class has 4 subclasses:
- SQS: handles sending, deleting, and receiving SQS messages.
- S3: handles uploading and receiving files from S3.
- EC2: handles launching, terminating and getting information about EC2 instances.
- DynamoDB: handles all functions needed for our Database used by the Manager module.

**3.4.2 Config**  
The app has cross-defined constants which can be found in common-utils/Config.java. This way we don't hardcode constants into our code.

**3.4.3 StanfordParser**  
This class handles the parsing done by the workers.  
This way, all the heavy-lifting-code that does the parsing itself is seperated from Worker logic.


## 4. Distributed-System Properties

Let us now delve further into the system’s design and the features it offers.

### **Security Considerations**

- **IAM Profile Instance**  
  We use an IAM Profile Instance when we start each ec2 instance, so every instance has access to AWS SDK, without sending credentials or pem files.

- **No Hard-Coded Secrets**  
  Credentials are never embedded in source code, configuration files, or user-data scripts.  
  All access keys remain outside the repository.


### 4.1 Scalability

The system is highly scalable due to:

- **SQS as a distributed task queue:**  
  Provides natural load balancing across Workers.

- **Stateless Workers:**  
  Additional Workers can be launched without coordination - all the information needed to process a task is inside the SQS message itself

- **Workers also scale internally:**  
  Each Worker uses multiple threads to parse different segments of a file in parallel, so even a single Worker increases throughput by exploiting multicore processing.

- **CPU-dependent parsing performance:**  -
  The parsing stage is the only component whose performance scales *directly* with the number of CPU threads available on the underlying EC2 instance.  
  More vCPUs would allow each Worker to run more parsing threads efficiently, significantly accelerating NLP analysis.

- **Dynamic EC2 scaling by Manager:**  
  Manager computes the number of Workers required per job based on workload - and if not the limitation of 19 workers could add even more if needed

- **Concurrent Manager processing:**  
  Manager uses a thread pool for handling multiple Local Applications in parallel.

- **Cloud-native autoscaling characteristics:**  
  S3 and SQS handle unlimited throughput - meaning with unnlimited resources it can support a large amount of people.

The architecture can scale to thousands or millions of tasks, limited only by AWS quotas and budget.

---

### 4.2 Fault-Tolerance & Persistence

The system ensures correctness even under partial failures:

- **Visibility Timeouts:**  
  Prevent duplicate processing via different workers and guarantee re-delivery if a Worker stalls or dies.

- **DynamoDB persistent state:**  
  Manager does not store progress in memory; state is recoverable even if Manager restarts.

- **Idempotent result assembly:**  
  Append-only construction of the summary file avoids corruption.

- **Stateless Worker design:**  
  Workers can be created or destroyed at any time and be depended only on the message in queue.

- **Messages are not deleted until processing is complete:**  
  Workers extend message visibility but **do not delete the message** from the queue until the analysis is fully finished.  
  This guarantees that unprocessed or partially processed tasks remain safely in SQS and can be picked up by another Worker at any time.

- **Periodic Manager re-check in LocalApp:**  
  The Local Application ensures that a Manager shutdown does not leave the system without an ensuring creation of new one.

### 4.3.**Threads in the Application**

The system uses threads selectively and purposefully to improve concurrency where beneficial, while avoiding unnecessary overhead:

- **1. Manager-Level Threading**  
  The Manager employs a **thread pool** to process multiple Local Applications concurrently.  
  Each incoming job from **LocalsOutput** is handled by a separate thread, enabling the Manager to:
  - parse different input files in parallel,
  - generate SQS messages concurrently,
  - and scale efficiently when multiple clients submit work at the same time.

- **2. Worker-Level Threading (Used Carefully)**  
  Workers support multithreaded parsing by dividing a file into segments and allowing multiple threads to process these segments concurrently.  
  However, the Stanford Parser is computationally heavy, and excessive threading on a single Worker may:
   - increase context-switching overhead,
   - cause heap pressure,
   - and **stall parsing instead of speeding it up**.

  For this reason, Workers use **only a small, controlled number of threads**, ensuring that performance improves without causing memory or CPU contention.

- **3. Visibility Extender Thread in Workers**  
  Each Worker spawns a **dedicated visibility-extender thread** to renew the SQS message visibility timeout while parsing is ongoing.  
  This ensures:
   - the message remains invisible to other Workers during long analysis jobs,
   - tasks are never duplicated,
   - and the Worker does not depend on synchronous blocking operations.

- **4. LocalApp Manager Checker Thread**  
  The Local Application runs a background thread that periodically checks whether the Manager EC2 instance is alive.  
  If the Manager fails or terminates unexpectedly, this thread ensures that a new Manager instance is launched.

---

Together, these threading mechanisms allow the system to remain responsive, concurrent, and fault-tolerant—while maintaining efficiency by avoiding over-threading in components where it would hinder performance.

### 4.4 Clear Separation of Responsibilities

- **LocalApp:** Client orchestration
- **Manager:** Scheduling, parallelization, resource provisioning, aggregation
- **Workers:** Independent computation units
- **SQS:** Communication fabric, decoupling producers and consumers
- **S3:** Durable object storage
- **DynamoDB:** Persistent progress tracking

This design avoids both under- and over-centralization.

---

### 4.5 Correct Termination

The system follows a strict termination sequence:

1. Local Application sends `"terminate"` to **LocalsOutput**.

2. Manager switches into *termination mode*.

3. Manager waits for:
   - all tasks to be absorbed by Workers,
   - all results to be returned,
   - both work queues to reach zero messages.

4. Manager signals Workers through **Workers_Tasks_Incoming**.

5. Manager terminates all Worker instances.

6. Manager exits, completing shutdown.

This ensures **no Worker is terminated prematurely**, no message is lost, and no resources are left running.

### 4.6 **System Limitations and Efficient Utilization**

The system is explicitly designed with awareness of the limitations of the AWS Lab environment and uses its available resources to their fullest potential:

- **Single-Manager Constraint:**  
  The architecture operates with only **one Manager EC2 instance**, as permitted by the lab environment.  
  If additional machines or CPUs were available, the system could be extended to support **multiple Managers**, enabling higher throughput and simultaneous handling of a larger number of Local Applications.

- **Budget-Constrained Instance Types:**  
  Due to cost restrictions, the system uses modest EC2 instance types (e.g., `t3.micro`, `t3.large`).  
  With a larger budget, we could allocate Workers and the Manager more powerful machines—with higher CPU counts, more memory, and increased network bandwidth—significantly improving parsing performance, parallelism, and overall processing speed.

- **Optimized Worker Behavior:**  
  Worker multithreading is tuned to match the limited CPU and memory of the instances; excessive threading would degrade performance rather than improve it.

### 4.7 Distributed Behaviour
The only thing waiting in the system is the local app & manager for the worker to finish parsing the files. Except that, the information is distributed correctly and nothing waits another.

## 5.1. AWS Resource Configuration (Summary)

- **Manager Instance:**
- AMI: `ami-0cae6d6fe6048ca2c`
- Type: `t3.micro`


- **Worker Instances:**
- AMI: same as Manager
- Type: `t3.large`
- Dynamically provisioned based on load
- Maximum simultaneously: 18


- **AWS Region:** `us-east-1`
- **JAR Deployment:** via S3 jar bucket and EC2 user-data script
- **Credentials Handling:** secure SCP of AWS credentials into each EC2 instance (never embedded in code)

## 5.2. How to run

- here are a few steps for how to run the system:
   1. start the AWS lab
   2. Change the aws_folder_path in the config file to a path resembling  - "C:\\Users\\(username)\\.aws"
   3. go to the AWS details and retrieve the new credentials of the running lab. save these credentials in a file named credentials in the absolute path "C:\\Users\\(username)\\.aws" on your computer.
   4. create a new bucket for the jar files (the manager and worker) and make sure to change the name JAR_BUCKET in the config file accordingly.
      Put the two jars in the created bucket.
   5. run the file localApp on your local computer with the correct arguments: input_file output_file n [terminate]

## 5.3. Performance Notes

- Number of URLs processed: **9**
- Number of Workers launched: **8**
- n parameter: **1**
- Total runtime on the sample input: **55 Minutes**

## 6. Conclusion

The system fully satisfies all requirements of the assignment:

- Complete end-to-end distributed processing pipeline
- Scalable, concurrent, and efficient architecture
- Robust fault-tolerance through SQS semantics and DynamoDB persistence
- Clean separation between roles of LocalApp, Manager, and Workers
- Compliance with cloud computing best practices
- Correct handling of multi-client scenarios and graceful termination

This README documents the project’s architecture, design rationale, and operation at a professional, academic standard suitable for evaluation.

---