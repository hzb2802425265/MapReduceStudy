package com.hzb.mapreduce.outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 10:20
 */
public class LogReducer extends Reducer<Text, NullWritable,Text, NullWritable> {
    //仍然没有处理逻辑,直接输出.但是要注意保证数据的完整输出
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//       防止有相同的数据,迭代写出
        for (NullWritable value : values) {
            context.write(key,value);
        }


    }
}
