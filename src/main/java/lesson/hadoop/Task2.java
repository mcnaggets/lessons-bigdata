package lesson.hadoop;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Task2 {

    private static Map<String, LongAdder> counter = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        long time = System.currentTimeMillis();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), new Configuration());
        read(fs);
        write(fs);
        System.out.printf("Total time %s\n", time(time));
    }

    private static void read(FileSystem fs) throws InterruptedException, IOException {
        long time = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        pool.invokeAll(
                Stream.of(fs.listStatus(new Path("/test0002"))).sorted((l1, l2) -> -Long.compare(l1.getLen(), l2.getLen()))
                        .map(s -> (Callable<Void>) () -> readFile(fs, s.getPath())).collect(toList()));
        pool.shutdown();
        System.out.printf("Read time %s\n", time(time));
    }

    private static void write(FileSystem fs) throws IOException {
        long time = System.currentTimeMillis();
        FSDataOutputStream outputStream = fs.create(new Path("/test0002_out/bid_result.txt"), true);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            counter.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e->e.getValue().sum()))
                    .entrySet().stream().sorted((e1,e2)->-Long.compare(e1.getValue(),e2.getValue())).forEach(
                    e -> writeString(writer, e)
            );
        }
        System.out.printf("Write time %s\n", time(time));
    }

    private static String time(long time) {
        return (System.currentTimeMillis() - time) / 1000 + "s";
    }

    private static void writeString(BufferedWriter writer, Map.Entry<String, Long> entry) {
        try {
            writer.write(entry.getKey() + "\t" + entry.getValue());
            writer.newLine();
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
    }

    private static Void readFile(FileSystem fs, Path path) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new BZip2CompressorInputStream(fs.open(path))))) {
            long time = System.currentTimeMillis();
            System.out.printf("Processing path %s\n", path);
            String line = reader.readLine();
            while (line != null) {
                counter.computeIfAbsent(line.split("\t")[2], k -> new LongAdder()).increment();
                line = reader.readLine();
            }
            System.out.printf("Path %s processed in %s\n", path, (time(time)));
            return null;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
    }
}
