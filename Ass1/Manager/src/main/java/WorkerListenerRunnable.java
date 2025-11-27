import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
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

            Message messageFromWorker = AmazonUtils.SQS.receiveFirstMessage(workers_done_tasks_queue_url);
            if(messageFromWorker != null) {
                String[] message_split = messageFromWorker.body().split("\n");
                int local_id = Integer.parseInt(message_split[0]);
                String input_file_to_analyze_url = message_split[1];
                String worker_bucket_name = message_split[2];
                String analyzed_file_key = message_split[3];
                String requested_analysis = message_split[4];

                //Append to the S3 output file of the local the line:
                // <analysis type>: <input file link> <output file link>
                String analyzed_file_url = AmazonUtils.S3.getFileUrl(worker_bucket_name, analyzed_file_key);
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
                    AmazonUtils.SQS.sendMessage(locals_input_queue_url, messageToClient);

                    AmazonUtils.SQS.DeleteMessage(workers_done_tasks_queue_url, messageFromWorker);
                    AmazonUtils.DynamoDB.deleteEntry(local_id);

                }
            }
        }
    }
}
