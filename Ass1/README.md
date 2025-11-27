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