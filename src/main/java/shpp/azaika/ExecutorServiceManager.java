package shpp.azaika;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceManager {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorServiceManager.class);
    public static void shutdownExecutor(ExecutorService executor, String name, long timeout, TimeUnit unit) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                logger.warn("{} threads did not finish in time. Forcing shutdown.", name);
                executor.shutdownNow();
            } else {
                logger.info("{} shutdown successfully.", name);
            }
        } catch (InterruptedException e) {
            logger.error("Waiting for {} threads to terminate was interrupted.", name, e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

