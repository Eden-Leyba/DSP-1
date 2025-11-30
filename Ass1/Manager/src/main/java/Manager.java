import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

public class Manager {

    static Thread workersThread;
    static Thread localsListenerThread;

    public static void main(String[] args)
    {
        AmazonUtils.SQS.buildQueue(Config.workers_done_queue_name);
        AmazonUtils.SQS.buildQueue(Config.workers_incoming_queue_name);

        String workers_done_queue_url =  AmazonUtils.SQS.getQueueURL(Config.workers_done_queue_name);
        String workers_incoming_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_incoming_queue_name);
        String locals_output_queue_url = AmazonUtils.SQS.getQueueURL(Config.locals_output_queue_name);
        String locals_input_queue_url = AmazonUtils.SQS.getQueueURL(Config.locals_input_queue_name);

        int nThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService locals_thread_pool = Executors.newFixedThreadPool(nThreads);

        //Write a terminate=false file
        try (FileWriter writer = new FileWriter("terminate.txt")) {
            writer.write("false");
        } catch (IOException e) {
            e.printStackTrace();
        }

        LocalsListenerRunnable localsListenerRunnable = new LocalsListenerRunnable(
                locals_output_queue_url,
                workers_incoming_queue_url,
                locals_thread_pool
        );

        WorkerListenerRunnable workerListenerRunnable = new WorkerListenerRunnable(
                workers_done_queue_url, locals_input_queue_url, workers_incoming_queue_url
        );

        localsListenerThread = new Thread(localsListenerRunnable);
        workersThread = new Thread(workerListenerRunnable);

        localsListenerThread.start();
        workersThread.start();

        try {
            localsListenerThread.join();
            workersThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        //Both threads finished
        AmazonUtils.SQS.deleteAllMessages(workers_done_queue_url);
        AmazonUtils.SQS.deleteAllMessages(workers_incoming_queue_url);

        //Terminate all ec2 workers
        List<String> running_workers_ids = AmazonUtils.EC2.getIdsEC2WithTagRunning(Config.instances_tag_name, Config.worker_role_value);
        for(String id : running_workers_ids) {
            AmazonUtils.EC2.terminateInstance(id);
        }

    }

}
