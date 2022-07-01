package com.hzb.mapreduce.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 10:16
 */
public class Driver {
    /**outputFormat说明:
     * outputformat是mr的输出类,所以的输出类都实现了这个接口.
     * mr中默认的输出类是TextOutputFormat
     * 在实际的应用中Driver中可以设置自定义的outputformat类,通过自定义实现mr程序想mysql,hbase等介质中的输出.
     * 具体的输出逻辑写在自定义的类中.
     * 自定义需要自写一个类继承FileOutputFormat类重新其getRecordWrite方法,这里还有重写一个继承RecordWrittabl的类
     * 在这个类中重写wrtie方法和cloes方法.write方法中实现自定义的输出逻辑,close中关闭write中打开的各种流资源
     *
     *
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        获取配置文件
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置mr的三大程序类
        job.setJarByClass(Driver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        //设置输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//#############################################################
        //设置自定义的outputformat
        job.setOutputFormatClass(LogOutputFormat.class);
//#############################################################
        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\inputOutputFormat"));
        //虽然设置了自定义的输出类，但是这里的路径是为了输出successful后的标记文件和校验文件，必须设置
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\OutputFormat"));
        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);





    }
}
