import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.Map;

public class WorkerListenerRunnable implements Runnable {
    String workers_done_tasks_queue_url;
    String locals_input_queue_url;

    public WorkerListenerRunnable(String workers_done_tasks_queue_url, String locals_input_queue_url) {
        this.workers_done_tasks_queue_url = workers_done_tasks_queue_url;
        this.locals_input_queue_url = locals_input_queue_url;
    }

    @Override
    public void run() {
        final SqsClient sqs = SqsClient.builder()
                .region(Config.region)
                .build();

        while(true) {
            ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                    .queueUrl(workers_done_tasks_queue_url)
                    .waitTimeSeconds(20)      // long polling (max allowed)
                    .maxNumberOfMessages(1)  // batch receive
                    .build();

            String message = AmazonUtils.SQS.receiveFirstMessage(workers_done_tasks_queue_url).body();
            if(message != null) {
                int local_id = Integer.parseInt(message.split("\n")[0]);
                String input_file_to_analyze_url = message.split("\n")[1];
                String worker_bucket_name = message.split("\n")[2];
                String analysed_file_key = message.split("\n")[3];
                String requested_analysis = message.split("\n")[4];

                //Append to the S3 output file of the local the line:
                // <analysis type>: <input file link> <output file link>
                String analyzed_file_url = AmazonUtils.S3.getFileUrl(worker_bucket_name, analysed_file_key);
                String new_summary_line = requested_analysis + ": " + input_file_to_analyze_url + " " + analyzed_file_url;

                Map<String, AttributeValue> item = AmazonUtils.DynamoDB.getEntry(local_id);
                String summary_bucket = item.get("output_summary_s3_bucketname").s();
                String summary_key = item.get("output_summary_s3_key").s();

                AmazonUtils.S3.appendToS3File(summary_bucket, summary_key, new_summary_line);

                //Increment num_done in locals
                AmazonUtils.DynamoDB.incrementNumDone(local_id);
                if(Integer.parseInt(item.get("num_urls").n()) == Integer.parseInt(item.get("num_done").n()) + 1) {
                    //Local is done
                    String messageToClient = local_id + "\n" + summary_bucket + "\n" + summary_key;
                    AmazonUtils.SQS.sendMessage(workers_done_tasks_queue_url, messageToClient);
                }
            }
        }
    }
}
