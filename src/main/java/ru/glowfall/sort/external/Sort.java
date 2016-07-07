package ru.glowfall.sort.external;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class Sort {
    private static final Logger log = LoggerFactory.getLogger(Sort.class);

    private final Options opt;

    private Sort(Options options) {
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

        new Sort(options).run();
    }

    private void run() {
        log.info("Sorting input file: " + opt.inputFile);
        final long start = System.currentTimeMillis();

        // Разделяем входной файл на сортированные части
        final List<File> tmpFiles = splitAndSort();

        List<ReaderWrapper> readers = Collections.emptyList();
        try {
            // Оборачиваем входные потоки в обёртки, поддерживающие ордеринг по первой строке
            readers = tmpFiles.stream().map(ReaderWrapper::new).collect(Collectors.toList());

            // Объединяем потоки в один выходной файл, используя K-way merging
            merge(readers);
        } finally {
            for (ReaderWrapper reader : readers) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error("Cant close reader", e);
                }
            }
        }

        log.info("Sorting completed in: " + (System.currentTimeMillis() - start) + "ms");
    }

    @NotNull
    private List<File> splitAndSort() {
        try (BufferedReader br = new BufferedReader(new FileReader(opt.inputFile))) {
            final List<File> tmpFiles = splitAndSort(br);
            log.info("Created " + tmpFiles.size() + " temporary files");
            return tmpFiles;
        } catch (IOException e) {
            log.error("Cant open input file", e);
            return Collections.emptyList();
        }
    }

    @NotNull
    private List<File> splitAndSort(BufferedReader br) throws IOException {
        final List<File> tmpFiles = new ArrayList<>();

        String line;
        do {
            // Внешний цикл крутим по файлам-чанкам
            final List<String> lines = new ArrayList<>();
            int currentChunkSize = 0;
            while ((line = br.readLine()) != null) {
                // Во внутреннем цикле считываем строки, пока не наберется очередной чанк
                lines.add(line);
                currentChunkSize += line.length();
                if (currentChunkSize > opt.chunkSize) {
                    break;
                }
            }
            tmpFiles.add(sortAndSave(lines));
        } while (line != null);

        return tmpFiles;
    }

    private File sortAndSave(List<String> lines) throws IOException {
        final File file = File.createTempFile("sort.chunk", ".tmp");
        file.deleteOnExit();
        Collections.sort(lines);
        Files.write(file.toPath(), lines, Charset.forName("UTF-8"));
        return file;
    }

    private void merge(List<ReaderWrapper> readers) {
        try (PrintWriter writer = new PrintWriter(opt.outputFile, "UTF-8")) {
            final PriorityQueue<ReaderWrapper> queue = new PriorityQueue<>(readers);
            while (!queue.isEmpty()) {
                final ReaderWrapper reader = queue.poll();
                final String header = reader.getHeader();
                if (header != null) {
                    // Печатаем заголовок и возвращаем ридера в очередь с новым приоритетом
                    writer.println(header);
                    queue.add(reader);
                }
            }
        } catch (IOException e) {
            log.error("Cant merge temporary files", e);
        }
    }

    private static class ReaderWrapper implements AutoCloseable, Comparable<ReaderWrapper> {
        private BufferedReader reader;
        private String header;

        ReaderWrapper(File file) {
            try {
                this.reader = new BufferedReader(new FileReader(file));
                readHeader();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        String getHeader() throws IOException {
            final String header = this.header;
            if (header != null) {
                readHeader();
            }
            return header;
        }

        private void readHeader() throws IOException {
            this.header = this.reader.readLine();
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
        }

        @Override
        public int compareTo(ReaderWrapper o) {
            return ObjectUtils.compare(header, o.header);
        }
    }

    private static class Options {
        @Parameter(names = "-c", required = true, description = "Chunk size")
        int chunkSize;

        @Parameter(names = "-i", required = true, description = "Input file to sort")
        String inputFile;

        @Parameter(names = "-o", required = true, description = "Output file")
        String outputFile;
    }
}
