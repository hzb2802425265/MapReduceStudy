package com.hzb.mapreduce.writableComparablePartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 22:00
 */
public class PhoneReducer  extends Reducer<PhoneFile,Text,Text,Text> {
        Text phoneobjcet= new Text();
//        PhoneFile pf = new PhoneFile();

    @Override
    protected void reduce(PhoneFile key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        phoneobjcet.set(key.toString());
        for (Text value : values) {
          context.write(value,phoneobjcet);
        }

    }
}
