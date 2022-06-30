package com.hzb.mapreduce.partitioner;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 18:38
 */
public class PhoneMapper extends Mapper<LongWritable, Text,Text, PhoneFile> {

    Text phone = new Text();
    PhoneFile pf =new PhoneFile();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =value.toString();
        String[] informations= line.replace("\t\t\t","\t\t").split("\t");
        phone.set(informations[1]);//key
        pf.setUpFlow(Long.parseLong(informations[4]));
        pf.setDownFlow(Long.parseLong(informations[5]));
        pf.setSumFlow();

        context.write(phone,pf);

    }
}
