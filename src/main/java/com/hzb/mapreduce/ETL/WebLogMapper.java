package com.hzb.mapreduce.ETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 17:30
 */
public class WebLogMapper extends Mapper<LongWritable,Text, Text, NullWritable> {
        Text text =new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean result = paselog(line,context);

        if(result){
            text.set(line);
            context.write(text,NullWritable.get());
        }else {
            return;
        }


    }

    private boolean paselog(String line, Context context) {

        if(line.length()>100){
            return true;
        }else{
            return false;
        }
    }
}
