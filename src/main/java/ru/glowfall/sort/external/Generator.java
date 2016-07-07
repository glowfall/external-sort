package ru.glowfall.sort.external;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class Generator {
    private static final Logger log = LoggerFactory.getLogger(Generator.class);

    private final Options opt;

    private Generator(Options options) {
        this.opt = options;
    }

    public static void main(String[] args) {
        final Options options = new Options();

        final JCommander jCommander = new JCommander(options);
        try {
            jCommander.parse(args);
        } catch (ParameterException e) {
            jCommander.usage();
            return;
        }

        new Generator(options).run();
    }

    private void run() {
        log.info("Generating random-string file with size " + opt.fileSize + " to: " + opt.filePath);
        final long start = System.currentTimeMillis();

        try (PrintWriter writer = new PrintWriter(opt.filePath, "UTF-8")) {
            for (int i = 0; i < opt.fileSize; ) {
                // Длина очередной строки - либо оставшийся кончик файла, либо случайное число от 0 до maxLineLength
                final int randomLen = ThreadLocalRandom.current().nextInt(10, opt.maxLineLength);
                final int lineLen = Math.min(randomLen, opt.fileSize - i);
                writer.println(RandomStringUtils.randomAlphanumeric(lineLen));
                i += lineLen;
            }
        } catch (IOException e) {
            log.error("Cant write to output file", e);
        }

        log.info("Generation completed in: " + (System.currentTimeMillis() - start) + "ms");
    }

    private static class Options {
        @Parameter(names = "-f", required = true, description = "Size of generated file in bytes")
        int fileSize;

        @Parameter(names = "-l", required = true, description = "Maximum line length")
        int maxLineLength;

        @Parameter(names = "-o", required = true, description = "Output file")
        String filePath;
    }
}
