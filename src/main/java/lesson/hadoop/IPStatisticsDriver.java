package lesson.hadoop;

import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class IPStatisticsDriver extends Configured implements Tool {

    public static final String GROUP_NAME = "browsers";

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("IPStatistics example");
        job.setJarByClass(IPStatisticsDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TotalCountWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/test0003/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/test0003/out"));

        job.setMapperClass(TotalCountMapper.class);
        job.setReducerClass(TotalCountReducer.class);
        job.setCombinerClass(TotalCountReducer.class);

        boolean success = job.waitForCompletion(true);

        System.out.println("Browser counters:");
        job.getCounters().getGroup(GROUP_NAME).forEach(c->
                System.out.println(c.getDisplayName() + ":" + c.getValue()));

        return success ? 0 : 1;
    }

    public static class TotalCountMapper extends Mapper<Object, Text, Text, TotalCountWritable> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split(" ");
            Browser browser = Browser.parseUserAgentString(value.toString());
            context.getCounter(GROUP_NAME, browser.getName()).increment(1);
            Integer total = getTotal(split[9]);
            context.write(new Text(split[0]), new TotalCountWritable(1, total));
        }

        private Integer getTotal(String s) {
            try {
                return Integer.valueOf(s);
            } catch (Exception x) {
                return 0;
            }
        }

    }

    public static class TotalCountReducer extends Reducer<Text, TotalCountWritable, Text, TotalCountWritable> {


        @Override
        protected void reduce(Text key, Iterable<TotalCountWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            long total = 0;
            for (TotalCountWritable value : values) {
                count += value.getCounter();
                total += value.getTotal();
            }
            context.write(key, new TotalCountWritable(count, total));
        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec",
                SnappyCodec.class.getName());
        int exitCode = ToolRunner.run(conf, new IPStatisticsDriver(), args);
        System.exit(exitCode);
    }

}
