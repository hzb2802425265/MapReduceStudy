package com.hzb.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-28 17:05
 */
public class Driver  {
    /**MR驱动主程序的相关说明：
     * 主程序的创建主要包含如下内容：
     * 1 获取配置信息以及获取job对象
     * 2 关联本Driver程序的jar
     * 3 关联Mapper和Reducer的jar
     * 4 设置Mapper输出的kv类型
     * 5 设置最终输出kv类型
     * 6 设置输入和输出路径
     * 7 提交job
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//      1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//      2 关联本Driver程序的jar (主程序类)
        job.setJarByClass(Driver.class);
//      3 关联Mapper和Reducer的jar(程序类)
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
//      4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//      5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//      6 设置job的输入和输出路径
        //win本地路径
        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\in"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\out"));
        //打jar包在Linux中执行自己输入路径
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//      7 提交job
        boolean result = job.waitForCompletion(true);//提交job，true查看完整日志
        System.exit(result ? 0 : 1);//退出

    }
}
