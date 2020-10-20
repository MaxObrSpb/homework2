package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Exchanger;
import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private final String resultFileName;
    private final Exchanger<Pair<?, ?>[]> exchanger;

    public FileWriter(String resultFileName, final Exchanger<Pair<?, ?>[]> exchanger) {
        this.resultFileName = resultFileName;
        this.exchanger = exchanger;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

        BufferedWriter writer = null;
        try {
            final File folder = new File("results/");
            if (!folder.exists()) {
                folder.mkdir();
            }
            final File resultDir = new File("results/" + resultFileName);
            writer = new BufferedWriter(new java.io.FileWriter(resultDir));
            while (true) {
                Pair<?, ?>[] array;
                array = exchanger.exchange(new Pair[FileProcessor.CHUNK_SIZE]);
                boolean containsNull = Arrays.stream(array).map(item -> item.getRight()).anyMatch(right -> (Integer) right == -1);
                for (Pair<?, ?> pair : array) {
                    if ((Integer) pair.getRight() != -1){
                        writer.write(pair.getLeft() + " " + pair.getRight() + "\n");
                    }
                }
                if (containsNull) break;
            }
        } catch (IOException | InterruptedException e) {
            logger.error("", e);
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                logger.error("", e);
            }
        }
        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
