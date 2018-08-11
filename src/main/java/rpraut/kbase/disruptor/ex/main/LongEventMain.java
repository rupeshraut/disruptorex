package rpraut.kbase.disruptor.ex.main;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadFactory;

public class LongEventMain {

    private static void handleEvent(LongEvent event, long sequence, boolean endOfBatch) {
        System.out.println("handle " + event);
    }

    private static void handlePersistEvent(LongEvent event, long sequence, boolean endOfBatch) {
        System.out.println("persist - " + event);
    }

    private static void clearEvent(LongEvent event, long sequence, boolean endOfBatch) {
        System.out.println("clearing event " + event);
        event = null;
    }

    private static void translate(LongEvent event, long sequence, ByteBuffer buffer) {
        event.set(buffer.getLong(0));
    }

    public static void main(String[] args) throws Exception {

        // Executor that will be used to construct new threads for consumers
        ThreadFactory executor = DaemonThreadFactory.INSTANCE;

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 4096;

        // Construct the Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent::new, bufferSize, executor, ProducerType.SINGLE, new BlockingWaitStrategy());

        // Connect the handler
        disruptor.handleEventsWith(LongEventMain::handleEvent);

        disruptor.handleEventsWith(LongEventMain::handlePersistEvent).then(LongEventMain::clearEvent);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l++) {
            bb.putLong(0, l);
            ringBuffer.publishEvent(LongEventMain::translate, bb);
            Thread.sleep(100);
        }
    }
}