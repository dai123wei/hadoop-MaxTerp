package com.dw.MT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import common_tool.HdfsDAO;

public class MainJob {
	
    public static void main(String[] args) throws Exception {

        if(2 != args.length) {
            System.out.println("Usage: MaxTemperature<input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        
        HdfsDAO.rmr(args[1], "hdfs://localhost:9000/", conf);
        
        Job job = new Job(conf, "temperature");
        job.setJarByClass(MainJob.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/" + args[1]));

        System.exit(job.waitForCompletion(true)?1:0);
    }
}
