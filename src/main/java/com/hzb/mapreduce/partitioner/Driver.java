package com.hzb.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 18:38
 */
public class Driver {
    /**partition的说明：
     * 关于分区，就是和Reuduce分不开的，当需求需要将数据按照某种特征的分类汇总后输出到不同文件时，需要设置分区。
     * 在不设置reuceTask时默认的reuduce个数是1，hadoop不会对数据进行分区，再设置reduce数量后，
     * hadoop默认采用的是根据key的hashcode值对reduce个数取余的方式来对数据进行分区的（大体上如此），最终分区数量等于reduce数量。
     * 可以通过自写类继承partition<mapOutK,mapOutV>类,并重新getPartiton方法，自定义实现分区逻辑
     * 在主程序中需要指明MR程序的分区类，并设置reduce数量。
     * reduce的数量应等于分区数量，大于时运行不会报错但是多余的reduce输出的文件没有内容，小于分区数量报错，因为有的分区没有reduce处理，
     * 但设置1时可以运行，和默认时一致，不走partition分区类
     *
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Driver.class);
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneFile.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//############################################################
//        设置使用自写的分区类
        job.setPartitionerClass(PhonePartitioner.class);
//        设置reduceTask数量
        job.setNumReduceTasks(5);
//############################################################
        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\inputPhone"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\out22"));

        boolean result = job.waitForCompletion(true);//提交job，true查看完整日志
        System.exit(result ? 0 : 1);//结束


    }
}
