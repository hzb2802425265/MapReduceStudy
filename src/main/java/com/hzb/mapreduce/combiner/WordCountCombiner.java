package com.hzb.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 22:20
 */
public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable value : values) {
            sum+=value.get();
        }
        outV.set(sum);
        context.write(key,outV);//将reduec处理的结果《key,value》输出
    }
}
