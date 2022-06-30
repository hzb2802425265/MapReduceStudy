package com.hzb.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 22:17
 */
public class Driver  {
    /**Combiner的相关说明：
     * Combiner是MR中的一个特殊组件，它的父类是Reducer，且自定义的方式和写Reducer完全一致
     * 作用是在maoTask阶段对数据切片提取进行reduce的处理逻辑操作，比如求和这样原本<k1,1><k1,1>这样重复的就先变成了<k1,2>之后才从map输出
     * 通过这种方式，可以使得每一个数据切片提前完成reduce的操作，那么就能够减少网络传输的数据量以及shuffle阶段处理的数据量
     * 但是，前提是这种提前进行的部分reduce，在完成了后续真正的reduce后的结果和全部使用最终的的reduce处理的结果一致，才能使用。
     * 且如果符合使用条件，也不需要单独写一个combiner，直接将reduce设置为使用的combiner就行。
     *
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
//        ##########################################
//        设置自定义的Combiner
        job.setCombinerClass(WordCountCombiner.class);
//        ##########################################
//      6 设置job的输入和输出路径
        //win本地路径
        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\input"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\outWord"));
        //打jar包在Linux中执行自己输入路径
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//      7 提交job
        boolean result = job.waitForCompletion(true);//提交job，true查看完整日志
        System.exit(result ? 0 : 1);//退出

    }
}
