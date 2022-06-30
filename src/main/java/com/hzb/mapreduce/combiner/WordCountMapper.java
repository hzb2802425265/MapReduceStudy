package com.hzb.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 22:17
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
    /**map的必要的解释说明
     * mapreduce的输入输出都是以<key,value>键值格式进行的
     * map程序必须继承Mapper类并指定输入输出的泛型<偏移量，输入的数据类型，输出key的数据类型1，输出的value数据类型2>
     * MR有自己的一套数据类型，除了String类型用Text表示外，其他的基本类型对应格式：Long=>LongWritable,Int=>IntWritable
     * map的数据处理逻辑是在Mapper类的map()方法中进行，所以必须重写map方法
     * map方法是对《k，v》进行处理，每个key,value就会执行一次map()函数
     *
     */
    Text k =new Text();
    IntWritable v =new IntWritable(1);//设置IntWritable类型的输出value

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        //数据是按照行输入的，每一行数据转换为了，<偏移量，line>的kv键值对
        String[] words=value.toString().split(" ");//将line的数据变成String类型进行空格切分
        for (String word : words) {
            k.set(word);//设置Text类型的输出key
            context.write(k,v);//将map的处理结果输出

        }


    }
}
