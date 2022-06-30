package com.hzb.mapreduce.writableComparablePartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 22:00
 */
public class Driver {
    /**分区内排序的案例说明
     * 整体上还是按key排序，mr上也符合排序的正常写法，
     * 不同的是通过设置分区，和reduce数量，使得每个分区内是有序的。
     * 且每个分区内的排序方式都是key位置的自定义类中重新的copareTo中的排序逻辑
     * 实际上就是对原有的全排序，进行分区了而已
     *
     *
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //加载配置文件
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);//生成job任务

        job.setJarByClass(Driver.class);//设置mr的主驱动类
        job.setMapperClass(PhoneMapper.class);//设置mr的map类
        job.setReducerClass(PhoneReducer.class);//设置mr的reduce类
        //map输出的k，v类型
        job.setMapOutputKeyClass(PhoneFile.class);
        job.setMapOutputValueClass(Text.class);
        //reduce输出的k，v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置分区和reduce数量
        job.setPartitionerClass(PhonePartition.class);
        job.setNumReduceTasks(5);
        //输入输出路径
        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\out"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\out444"));
        //job提交
        boolean result = job.waitForCompletion(true);//提交job，true查看完整日志
        System.exit(result ? 0 : 1);//结束


    }
}
