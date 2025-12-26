import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.*;

public class LocalsListenerRunnable implements Runnable {

    String locals_output_queue_url;
    String workers_input_queue_url;
    int locals_count;
    ExecutorService locals_thread_pool;

    boolean terminate;

    public LocalsListenerRunnable(String locals_output_queue_url, String workers_input_queue_url, ExecutorService locals_thread_pool) {
        this.locals_output_queue_url = locals_output_queue_url;
        this.workers_input_queue_url = workers_input_queue_url;
        this.locals_count = 0;
        this.locals_thread_pool = locals_thread_pool;
        this.terminate = false;
    }

    @Override
    public void run() {

        final SqsClient sqs = SqsClient.builder()
                .region(Config.region)
                .build();

        //create locals table
        AmazonUtils.DynamoDB.SetTableName("locals");

        while (!terminate) {
                ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                        .queueUrl(locals_output_queue_url)
                        .waitTimeSeconds(20)      // long polling (max allowed)
                    .maxNumberOfMessages(10)  // batch receive
                    .build();

            List<Message> messages = sqs.receiveMessage(request).messages();

            if (messages.isEmpty()) {
                continue; // no messages, loop again
            }

            for (Message msg : messages) {
                System.out.println("Received message: " + msg.body().replaceAll("\n", ";"));

                if(msg.body().equals(Config.msg_terminate_string)) {
                    terminate = true;
                    //rewrite terminate.txt
                    Path path = Paths.get("terminate.txt");
                    try {
                        if (!Files.exists(path)) {
                            throw new IOException("File does not exist: " + path);
                        }
                        String newContent = "true";
                        Files.write(path, newContent.getBytes());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    //send message to workers to terminate
                    AmazonUtils.SQS.sendMessage(workers_input_queue_url, Config.msg_terminate_string);
                    try {
                        AmazonUtils.EC2.terminateMyself();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                }
                else {
                    String bucket_name = msg.body().split("\n")[0];
                    String key = msg.body().split("\n")[1];
                    int n = Integer.parseInt(msg.body().split("\n")[2]);

                    long local_id = Long.parseLong(bucket_name.replace("dsp1-task1-", ""));
                    SubmitLocalTaskToThreadPool(local_id, bucket_name, key, n);
                }

                AmazonUtils.SQS.DeleteMessage(locals_output_queue_url, msg);
            }
        }
    }

    private void SubmitLocalTaskToThreadPool(long local_id, String bucket_name, String key, int n){
        Callable<Integer> locals_thread_task = new LocalThreadTask(
                bucket_name,
                key,
                local_id,
                workers_input_queue_url,
                n
        );

        Future<Integer> future_m = locals_thread_pool.submit(locals_thread_task);
        int k = AmazonUtils.EC2.getIdsEC2WithTagRunning(Config.instances_tag_name, Config.worker_role_value).size();

        if(k < Config.MAX_WORKER_INSTANCES) {
            int m = 0;
            try {
                m = future_m.get();
            } catch (ExecutionException e) {
                System.err.println("Local Thread threw an exception:\n" + e.getMessage());
            } catch (InterruptedException e) {
                System.err.println("Local Thread was interrupted:\n" + e.getMessage());
            }

            /*
            k = 10
            m = min(30,18)
            num = 18-10=8
             */

            //Start workers
            int num_workers_to_start = Math.min(m,Config.MAX_WORKER_INSTANCES) - k;
            String amiId = "ami-0cae6d6fe6048ca2c";
            try {
                AmazonUtils.EC2.LaunchMultipleInstances(
                        amiId,
                        InstanceType.T3_LARGE,
                        Config.instances_tag_name, Config.worker_role_value,
                        "jars-1763844625474", "Worker.jar",
                        num_workers_to_start, 1
                );
            } catch (Ec2Exception e) {
                if(e.awsErrorDetails().errorCode().equals("InsufficientInstanceCapacity")) {
                    System.err.println("Cannot run ec2 instance! Not enough EC2 Capacity!");
                }
            } catch (IOException | InterruptedException e) {
                System.err.println(e.getMessage());
            }
        }

    }

}
