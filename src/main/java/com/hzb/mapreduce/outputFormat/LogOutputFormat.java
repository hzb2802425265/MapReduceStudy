package com.hzb.mapreduce.outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 10:32
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {
    /**该类就是自定义的OutputFormat,继承自 FileOutputFormat<k, v>这里的k,v与reduce输出的类型一致
     *重写getRecordWriter方法,其中参数是mr的job,该方法返回的是一个RecordWrite<k, v>对象
     * 所以还要自定义一个继承RecordWrite<k, v>的类,并在其中重写write方法和coles方法,提供一个带参构造器,并在构造器中创建输出所需要的流
     *
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        LogRecordWriter logRecordWriter = new LogRecordWriter(job);
        return logRecordWriter;
    }
}
