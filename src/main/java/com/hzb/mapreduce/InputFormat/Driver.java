package com.hzb.mapreduce.InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 14:16
 */
public class Driver  {
    /**MR输入说明：
     * 默认是采用TextInputFormat进行数据输入的，他是<字节偏移量，line>的形式
     * 除此之外还有KeyValueTextInputFormat、NLineInputFormat、CombineTextInputFormat和自定义InputFormat等，适应不同的场景，具体的自行搜索
     * CombineTextInputFormat是解决小文件问题的：
     * 由于MR是分布式并行计算，他的并行对就是输入是切片产生的mapTaskde的数量，即切片数量，默认是一切片大小==一个块的大小
     * 实际的mr开发中切片大小可通过调节mapred-deflut.xml文件中的mapreduce.input.fileinputformat.split.minSize/maxSize控制
     *
     * maxSize小于blockSize时 ，切片变小为maxSize
     * minSize大于blockSize时 ，切片变大为minSize
     *
     *小文件过多时尝试使用Combine开头的输入类，能通过设置setMaxInputSplitSize对切片进行有效的合并。合并思想如下：
     * 首先会根据文件大小和设置的maxSize判断每个文件是否达到切片的大小，
     * 小于切片大小的文件，本身就是一个切片
     * 达到且大小在1-2倍之间则文件平分大小为两份，
     * 大于两倍的先切分一个maxSize大小的切片，剩余大小以此执行判断是在1-2倍还是大于2倍进行切分。
     * 所有的小文件切分完后，对每个切片进行判断，对小于maxSize的切片和下一个切片合并后再判断，
     * 直到切片大小大于等于maxSize，那么以上的几个切片合并为一个切片，即会产生一个mapTask
     * 合并前的切片只是进行合理的分块，以便后续合并成切片，分块不是真正的切片。
     *
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//      1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//####################################################################
// 设置使用的输入类, CombineTextInputFormat解决小文件问题
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,1024);
        //最小切片数量，文件小则按照规则合并
//######################################################################
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
        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\inputWord"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\outWord2"));
        //打jar包在Linux中执行自己输入路径
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//      7 提交job
        boolean result = job.waitForCompletion(true);//提交job，true查看完整日志
        System.exit(result ? 0 : 1);//退出

    }
}
