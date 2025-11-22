import java.util.concurrent.*;

public class Manager {

    static Thread workers_thread;
    static Thread localsListenerThread;

    public static  void main(String[] args)
    {
        AmazonUtils.SQS.buildQueue(Config.workers_output_queue_name);
        AmazonUtils.SQS.buildQueue(Config.workers_input_queue_name);
        String workers_output_queue_url =  AmazonUtils.SQS.getQueueURL(Config.workers_output_queue_name);
        String workers_input_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_input_queue_name);
        String locals_output_queue_url = AmazonUtils.SQS.getQueueURL(Config.locals_output_queue_name);

        int nThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService locals_thread_pool = Executors.newFixedThreadPool(nThreads);

        LocalsListenerRunnable localsListenerRunnable = new LocalsListenerRunnable(
                locals_output_queue_url,
                workers_input_queue_url,
                locals_thread_pool
        );

        localsListenerThread = new Thread(localsListenerRunnable);
        localsListenerThread.start();

        locals_thread_pool.shutdown();
    }

}
