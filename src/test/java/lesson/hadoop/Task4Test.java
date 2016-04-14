package lesson.hadoop;

import eu.bitwalker.useragentutils.OperatingSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Task4Test {
    MapDriver<Object, Text, CityBrowserWritable, BooleanWritable> mapDriver;
    ReduceDriver<CityBrowserWritable, BooleanWritable, CityBrowserWritable, FloatWritable> reduceDriver;
    MapReduceDriver<Object, Text, CityBrowserWritable, BooleanWritable, CityBrowserWritable, FloatWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Task4.Mapper mapper = new Task4.Mapper();
        Task4.Reducer reducer = new Task4.Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "99b6faed26d6a83bcbc74c6bf0c401ec\t20131019103201617\t1\tD8JLuM77wmF\tMozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t121.33.117.*\t216\t217\t1\tb5f57062ae7f4ba7f1489b9133b991d6\taaee1bce9f4986aaa57701cc5fd29010\tnull\tmm_30646014_3428401_13072799\t300\t250\tNa\tNa\t0\t7323\t394\t81\tnull\t2259\t13800,10059,11379,14273,10076,10077,10075,10074,10006,10024,13776,10111,10052,11944,16753,10063"));
        mapDriver.withOutput(new CityBrowserWritable(217, OperatingSystem.WINDOWS), new BooleanWritable(true));
        mapDriver.runTest();
    }

}