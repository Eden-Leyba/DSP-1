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

        LocalsListenerRunnable localsListenerRunnable = new LocalsListenerRunnable(
                locals_output_queue_url,
                workers_incoming_queue_url,
                locals_thread_pool
        );

        WorkerListenerRunnable workerListenerRunnable = new WorkerListenerRunnable(
                workers_done_queue_url, locals_input_queue_url
        );

        localsListenerThread = new Thread(localsListenerRunnable);
        workersThread = new Thread(workerListenerRunnable);

        localsListenerThread.start();
        workersThread.start();
    }

}
