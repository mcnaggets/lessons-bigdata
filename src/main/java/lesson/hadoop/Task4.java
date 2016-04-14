package lesson.hadoop;

import eu.bitwalker.useragentutils.OperatingSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Task4 extends Configured implements Tool {

    public static final BooleanWritable TRUE = new BooleanWritable(true);
    public static final BooleanWritable FALSE = new BooleanWritable(false);

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Task4");
        job.setJarByClass(Task4.class);

        job.setMapOutputKeyClass(CityBrowserWritable.class);
        job.setMapOutputValueClass(BooleanWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/test0004"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/test0004/out"));

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(6);
        job.setPartitionerClass(Partitioner.class);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new Task4(), args);
        System.exit(exitCode);
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, CityBrowserWritable, BooleanWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String bidPrice = split[19];
            int city = Integer.parseInt(split[7]);
            OperatingSystem os = OperatingSystem.parseUserAgentString(split[4]).getGroup();
            context.getCounter("OS", os.name()).increment(1);
            CityBrowserWritable cityBrowserWritable = new CityBrowserWritable(city, os);
            if (Long.parseLong(bidPrice) > 290) {
                context.write(cityBrowserWritable, TRUE);
            } else {
                context.write(cityBrowserWritable, FALSE);
            }
        }

    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<CityBrowserWritable, BooleanWritable, CityBrowserWritable, FloatWritable> {

        @Override
        protected void reduce(CityBrowserWritable key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
            int high = 0;
            int all = 0;
            for (BooleanWritable value : values) {
                if (value.get()) high++;
                all++;
            }
            context.write(key, new FloatWritable((float) high / all));
        }

    }

    public static class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<CityBrowserWritable, BooleanWritable> {
        @Override
        public int getPartition(CityBrowserWritable key, BooleanWritable value, int numPartitions) {
            if (numPartitions == 0) {
                return 0;
            }
            switch (key.os) {
                case WINDOWS: return 1 % numPartitions;
                case ANDROID: return 2 % numPartitions;
                case IOS: return 3 % numPartitions;
                case LINUX: return 4 % numPartitions;
                case MAC_OS:
                case MAC_OS_X: return 5 % numPartitions;
                default: return 0;
            }
        }
    }

    public static void main1(String[] args) {
        Partitioner partitioner = new Partitioner();
        int partition = partitioner.getPartition(new CityBrowserWritable(1, OperatingSystem.WINDOWS), TRUE, 2);
        System.out.println(partition);
    }
}
