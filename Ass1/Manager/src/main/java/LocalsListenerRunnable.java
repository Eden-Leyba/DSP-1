import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
import java.util.concurrent.*;

public class LocalsListenerRunnable implements Runnable {

    String locals_output_queue_url;
    String workers_input_queue_url;
    int locals_count;
    ExecutorService locals_thread_pool;

    public LocalsListenerRunnable(String locals_output_queue_url, String workers_input_queue_url, ExecutorService locals_thread_pool) {
        this.locals_output_queue_url = locals_output_queue_url;
        this.workers_input_queue_url = workers_input_queue_url;
        this.locals_count = 0;
        this.locals_thread_pool = locals_thread_pool;
    }

    @Override
    public void run() {

        final SqsClient sqs = SqsClient.builder()
                .region(Config.region)
                .build();

        while (true) {
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
                System.out.println("Received message: " + msg.body());
                int local_id = locals_count++;
                String bucket_name = msg.body().split("\n")[0];
                String key = msg.body().split("\n")[1];
                int n = Integer.parseInt(msg.body().split("\n")[2]);

                SubmitLocalTaskToThreadPool(local_id, bucket_name, key, n);

                AmazonUtils.SQS.DeleteMessage(locals_output_queue_url, msg);
            }
        }
    }

    private void SubmitLocalTaskToThreadPool(int local_id, String bucket_name, String key, int n){
        Callable<Integer> locals_thread_task = new LocalThreadTask(
                bucket_name,
                key,
                local_id,
                workers_input_queue_url,
                n
        );

        Future<Integer> future_m = locals_thread_pool.submit(locals_thread_task);
        int k = AmazonUtils.EC2.getNumEC2WithTagRunning(Config.instances_tag_name, Config.worker_role_value);

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
            for (int worker_num = 0; worker_num < num_workers_to_start; worker_num++) {
                String script = "echo 'This machine is running'";
                String amiId = "ami-0cae6d6fe6048ca2c";
                AmazonUtils.EC2.RunEC2InstanceWithSpecificTag(amiId, Config.instances_tag_name, Config.worker_role_value, script);
            }

        }

    }

}
