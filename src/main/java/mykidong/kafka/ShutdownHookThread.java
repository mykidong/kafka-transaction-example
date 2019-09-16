package mykidong.kafka;

/**
 * Created by mykidong on 2019-09-16.
 */
public class ShutdownHookThread<K, V> extends Thread{

    private ConsumerHandler<K, V> consumeHandler;

    private Thread mainThread;

    public ShutdownHookThread(ConsumerHandler<K, V> consumeHandler, Thread mainThread) {
        this.consumeHandler = consumeHandler;
        this.mainThread = mainThread;
    }

    @Override
    public void run() {
        this.consumeHandler.getConsumer().wakeup();

        // to make sure that WakeupException should be thrown before exit.
        this.consumeHandler.setWakeupCalled(true);
        try {
            mainThread.join();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }
}
