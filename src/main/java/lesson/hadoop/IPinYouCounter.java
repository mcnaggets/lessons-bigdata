package lesson.hadoop;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class IPinYouCounter {

    private static Map<String, LongAdder> counter = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        final AtomicInteger counter = new AtomicInteger();
        long time = System.currentTimeMillis();
        FileSystem fs = FileSystem.get(URI.create("hadoop://localhost:9000"), new Configuration());
        Executors.newFixedThreadPool(4).invokeAll(
                Stream.of(fs.listStatus(new Path("/ipinyou"), p-> counter.incrementAndGet()<=1)).map(
                        s -> (Callable<Void>) () -> readFile(fs, s.getPath())).collect(toList()));
        System.out.println(counter);
        System.out.printf("Total time %s", System.currentTimeMillis() - time);
    }

    private static Void readFile(FileSystem fs, Path path) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new BZip2CompressorInputStream(fs.open(path))), 500 * 1000 * 1024)) {
            long time = System.currentTimeMillis();
            System.out.printf("Processing path %s\n", path);
            String line = reader.readLine();
            while (line != null) {
                counter.computeIfAbsent(line.substring(51, 66), k -> new LongAdder()).increment();
                line = reader.readLine();
            }
            System.out.printf("Path %s processed in %s\n", path, (System.currentTimeMillis() - time));
            return null;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
    }
}
