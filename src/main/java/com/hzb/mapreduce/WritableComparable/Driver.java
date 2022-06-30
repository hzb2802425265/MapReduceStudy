package com.hzb.mapreduce.WritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 21:00
 */
public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Driver.class);
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);

        job.setMapOutputKeyClass(PhoneFile.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\out"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\out333"));

        boolean result = job.waitForCompletion(true);//提交job，true查看完整日志
        System.exit(result ? 0 : 1);//结束


    }
}
