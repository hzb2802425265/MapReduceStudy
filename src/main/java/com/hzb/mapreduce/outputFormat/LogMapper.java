package com.hzb.mapreduce.outputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 10:18
 */
public class LogMapper extends Mapper<LongWritable,Text, Text, NullWritable> {
    //根据案例的需求这里不做任何处理,只输出TextInputFormat输入的value,所以输出格式用NullWritable占位,表示空值
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//
        context.write(value,NullWritable.get());
    }
}
