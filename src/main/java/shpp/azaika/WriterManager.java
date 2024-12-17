package shpp.azaika;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;
import shpp.azaika.util.CsvWriter;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WriterManager {
    private static final Logger logger = LoggerFactory.getLogger(WriterManager.class);
    private final ExecutorService writerExecutor;

    public WriterManager(BlockingQueue<UserPojo> validQueue, BlockingQueue<UserPojo> invalidQueue) {
        writerExecutor = Executors.newFixedThreadPool(2);
        startWriters(validQueue, invalidQueue);
    }

    private void startWriters(BlockingQueue<UserPojo> validQueue, BlockingQueue<UserPojo> invalidQueue) {
        writerExecutor.submit(() -> writeUsersToCsv(validQueue, "valid_users.csv"));
        writerExecutor.submit(() -> writeUsersToCsv(invalidQueue, "invalid_users.csv"));
    }

    private void writeUsersToCsv(BlockingQueue<UserPojo> queue, String fileName) {
        try (CsvWriter writer = new CsvWriter(fileName)) {
            while (true) {
                UserPojo userPojo = queue.poll();
                if (userPojo == null && writerExecutor.isShutdown()) break;
                if (userPojo != null){
                    writer.write(userPojo);
                }
            }
        } catch (IOException e) {
            logger.error("Error writing to file: {}", fileName, e);
        }
    }

    public void shutdownWriterExecutor() {
        writerExecutor.shutdown();
    }
}