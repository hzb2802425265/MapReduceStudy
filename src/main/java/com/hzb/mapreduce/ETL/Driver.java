package com.hzb.mapreduce.ETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author huzhibin
 * @date 2022-07-01 17:28
 */
public class Driver {
    //ETL主要就是数据的抽取,转化,如果使用mr完成的话,只需要map就能完成,重点在于根据ETL的需求就每一次进入map的数据进行匹配,完成转化清洗等逻辑
    //一般时结合正则进行字符串的匹配,转化等

    public static void main(String[] args) throws Exception {

// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "E:\\JavaCode\\testTxt\\inputlog", "E:\\JavaCode\\testTxt\\outputlog" };

        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(Driver.class);

        // 3 关联map
        job.setMapperClass(WebLogMapper.class);

        // 4 设置最终输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reducetask个数为0
        job.setNumReduceTasks(0);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
