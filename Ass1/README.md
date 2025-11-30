# About the project
## Introduction

This project implements a distributed text-analysis system deployed on Amazon Web Services (AWS).
Each Local Application submits an input file containing multiple text‐file URLs together with their required analysis type (POS, Constituency, or Dependency).
Upon submission, the file is uploaded to Amazon S3 and a processing request is sent to the cloud.

A dedicated Manager EC2 instance orchestrates the distributed computation.
It retrieves the input file, decomposes it into individual analysis tasks, and launches Worker EC2 instances according to the workload and the parameter n (maximum number of files per worker).
Task distribution and result collection are performed through Amazon SQS queues.

Worker instances operate independently: each retrieves tasks from the queue, downloads the corresponding text file, performs the requested NLP analysis using the Stanford Parser, and uploads the processed output to S3.
The Manager aggregates all results belonging to a specific Local Application, constructs a single HTML summary file, stores it in S3, and notifies the originating Local Application via SQS.

This system was designed in accordance with the distributed-systems principles and requirements specified for the assignment.

It supports secure handling of AWS credentials, scalable execution under heavy workloads, persistent and fault-tolerant behavior in the presence of node failures, and correct recovery from stalled or interrupted processing through SQS visibility timeouts and message re-delivery.
The architecture also ensures correct behavior when multiple Local Applications run concurrently, proper division of responsibilities between Manager and Workers, and careful consideration of when threading is appropriate within individual components.
All of which will be discussed in detail in the following sections.

# How to run
here are afew steps for how to run the system:
1. stary yhe AWS lab
2. Change the aws_folder_path in the config file to a path resembling  - "C:\\Users\\(username)\\.aws"
3. go to the AWSdetals and retrive the new credentials of the running lab. save these credentials in a file named credentials in the absolute path "C:\\Users\\(username)\\.aws" on your computer.
4. make sure to have the labsuser.pem file, created when creating the coresponding keys, to be in the same folder as the credentials.
5. create a new bucket for the jar files (the maneger and worker) and make sure to change the name JAR_BUCKET in the config file acordingly.
   Put the two jars in the created bucket.
6. run the file localApp on your local computer

# Classes

### localApp
this is the local Application which

### Worker
The worker checks consistently the workers_incoming_queue for a new massege.
After recieving one it takes the url in the massage and the analysis requested and sends it to the parser with the relevnt info.
After the parser is done with its job, the worker takes the file with the analyzed information and uploads it to the
assigned bucket for future use by the manager.
The worker sends a message in the workers_done_queue including the local that requested the url and the path to the analyzed info.


# SQS Queue Protocols

### Locals Output
Locals (Many) -> Manager (One)  
Send a message once a local app is up and has an input file to process.   
The input file is uploaded to S3 in a bucket denoted *bucket* and key denoted *key*  
In addition, *n* is the number workers’ files ratio (max files per worker)  
Message format:  
bucket + "\n" + key + "\n" + n

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
local_id + "\n" + input_file_to_analyze_url + "\n" + worker_bucket_name + "\n" + analyzed_file_key + "\n" + requested_analysis

### Locals Input
Manager (One) -> Locals (Many)  
Done message from the manager to the application
Contains the local_id number (assigned by the Manager) and the location of the analysis summary file  
Message format:  
local_id + "\n" + summary_bucket + "\n" + summary_key