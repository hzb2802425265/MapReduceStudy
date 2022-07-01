package com.hzb.mapreduce.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 14:46
 */
public class Driver {
    /**reduce Join,就是对两个文件或者表,通过一个主键链接,以求的到某种统计结果
     * 这种处理方式就是以主键为key在map阶段对数据进行处理,这个案例是封装数据到类中.
     * 根据mr的特性,不同的mapTask处理的结果在reduce除会先进行归并,具有相同key的key,valuelist会在一起进入reduce处理
     * 因此利用这种特性在reduce端就可根据主键和特定的标识符(这里是封装在自定义类里面的文件名),以此来进行两个表的数据的join逻辑
     * 具体处理逻辑有需求而变,在reduce处对join和处理后的结果进行输出,这里是封装到类利用自定义类中重写toString()来输出
     * 这里的例子实在map中重写了的setup(),并从中利用切片是保存的文件路径,获取当前map处理的文件名,以此作为标识符
     * 但是这种join有很大的局限性,就是他的处理实在reduce端进行的,在实际的生成环境中reduce的资源一般都很少(相对与map),
     * Reduce端的处理压力太大，Map节点的运算负载则很低，资源利用率不高，且在Reduce阶段极易产生数据倾斜。
     *解决方式:在map端进join
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Driver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReduer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BeanTable.class);
        job.setOutputKeyClass(BeanTable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\inputtable"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\outtable"));
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);



    }
}
