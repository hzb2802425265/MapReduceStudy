package com.hzb.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-28 17:06
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
    /**Reduce的必要说明：
     * reduce程序必须基础Reducer<输入key,输入value,输出key,输出value>类，输入的k,v就是map的输出k,v一致
     * reduce的业务逻辑都在reudec()函数中，是针对key,valuelist的，每一组key,valuelis都会执行一次reduce
     */

    IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        int sum=0;
        for (IntWritable value : values) {
            sum+=value.get();
        }
        outV.set(sum);
        context.write(key,outV);//将reduec处理的结果《key,value》输出


    }
}
