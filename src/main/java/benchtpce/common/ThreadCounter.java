package benchtpce.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by carlosmorais on 12/12/2016.
 */
public class ThreadCounter {
    private static final Logger logger = LoggerFactory.getLogger(ThreadCounter.class);
    private final AtomicInteger counter = new AtomicInteger();

    public void increment(){
        this.counter.incrementAndGet();
        logger.debug("[INC] - Theads = "+getCounter());
    }

    public void decrement(){
        this.counter.decrementAndGet();
        logger.debug("[DEC] - Theads = "+getCounter());
    }

    public synchronized int getCounter() {
        return counter.get();
    }
}
