package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();
    private static final Exchanger<Pair<?, ?>[]> EXCHANGER = new Exchanger<>();
    private static final Pair<?, ?>[] ROWS = new Pair[CHUNK_SIZE];
    private static final Runnable action = () -> {
        try {
            EXCHANGER.exchange(Arrays.copyOf(ROWS, CHUNK_SIZE));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Arrays.fill(ROWS, new ImmutablePair<>("", -1));
    };

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        FileWriter fileWriter = new FileWriter(resultFileName, EXCHANGER);
        Thread writerThread = new Thread(fileWriter);
        writerThread.start();

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            int count = 0;
            ExecutorService service = Executors.newFixedThreadPool(CHUNK_SIZE);
            CyclicBarrier barrier = new CyclicBarrier(CHUNK_SIZE, action);
            while (scanner.hasNext()) {
                if (count >= CHUNK_SIZE ) {
                    count = 0;
                }
                String line = scanner.nextLine();
                service.execute(new Task(count, barrier, Optional.of(line)));
                count++;
            }
            if ((!scanner.hasNext()) && (CHUNK_SIZE > count)) {
                for (int j = 0; j < CHUNK_SIZE - count; j++){
                    service.execute(new Task(count - 1, barrier, Optional.empty()));
                }
            }
            if ((!scanner.hasNext()) && (CHUNK_SIZE == count)) {
                for (int j = 0; j < CHUNK_SIZE; j++){
                    service.execute(new Task(count - 1, barrier, Optional.empty()));
                }
            }
            service.shutdown();
        } catch (IOException exception) {
            logger.error("", exception);
        }

        try {
            writerThread.join();
        } catch (InterruptedException e) {
            logger.error("", e);
        }

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }

    static class Task implements Runnable{
        private final int i;
        private final CyclicBarrier barrier;
        private final Optional<String> line;

        Task(int i, CyclicBarrier barrier, Optional<String> line) {
            this.i = i;
            this.barrier = barrier;
            this.line = line;
        }

        @Override
        public void run() {
            try {
                if (line.isPresent()) {
                    Pair<String, Integer> pair = new LineCounterProcessor().process(line.get());
                    ROWS[i] = pair;
                } else {
                    ROWS[i] = new ImmutablePair<>("", -1);
                }
                barrier.await();
            } catch (BrokenBarrierException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
