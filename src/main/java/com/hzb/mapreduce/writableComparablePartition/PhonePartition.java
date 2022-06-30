package com.hzb.mapreduce.writableComparablePartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author huzhibin
 * @date 2022-06-30 22:00
 */
public class PhonePartition extends Partitioner<PhoneFile, Text> {


    @Override
    public int getPartition(PhoneFile phoneFile, Text text, int i) {

        String phoneHead = text.toString().substring(0,3);
        int partition;
        if("136".equals(phoneHead)){
            partition=0;
        }else if("137".equals(phoneHead)){
            partition=1;
        }else if("138".equals(phoneHead)){
            partition=2;
        }else if("139".equals(phoneHead)){
            partition=3;
        }else {
            partition=4;
        }

        return partition;
    }
}
