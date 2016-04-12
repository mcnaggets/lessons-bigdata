package lesson.hadoop;

import lesson.hadoop.Task3.TotalCountMapper;
import lesson.hadoop.Task3.TotalCountReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IPStatisticsDriverTest {
    MapDriver<Object, Text, Text, TotalCountWritable> mapDriver;
    ReduceDriver<Text, TotalCountWritable, Text, TotalCountWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, TotalCountWritable, Text, TotalCountWritable> mapReduceDriver;

    @Before
    public void setUp() {
        TotalCountMapper mapper = new TotalCountMapper();
        TotalCountReducer reducer = new TotalCountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapDriver.withOutput(new Text("ip1"), new TotalCountWritable(1, 40028));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<TotalCountWritable> values = new ArrayList<>();
        values.add(new TotalCountWritable(1, 400));
        values.add(new TotalCountWritable(5, 1200));
        values.add(new TotalCountWritable(2, 600));
        reduceDriver.withInput(new Text("ip"), values);
        reduceDriver.withOutput(new Text("ip"), new TotalCountWritable(8, 2200));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withAll(Arrays.asList(
                new Pair<Object, Text>(new LongWritable(), new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"")),
                new Pair<Object, Text>(new LongWritable(), new Text("ip2 - - [24/Apr/2011:04:20:20 -0400] \"GET /favicon.ico HTTP/1.1\" 200 318 \"-\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16")),
                new Pair<Object, Text>(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:23:21 -0400] \"GET /~strabal/grease/photo9/T927-5.jpg HTTP/1.1\" 200 3807 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""))
        ));
        mapReduceDriver.withAllOutput(Arrays.asList(
                new Pair<>(new Text("ip1"), new TotalCountWritable(1, 3807)),
                new Pair<>(new Text("ip2"), new TotalCountWritable(2, 390 + 318))
        ));
        mapReduceDriver.runTest();
    }
}