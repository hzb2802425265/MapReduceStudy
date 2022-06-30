package com.hzb.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author huzhibin
 * @date 2022-06-30 18:39
 */
public class PhonePartitioner  extends Partitioner<Text,PhoneFile> {
    /**自写Partition类的说明
     * 继承Partition<key,valeu>并重写getPartition()方法，
     * 在其中实现分区逻辑，并返回分区id由0==>>(numreduceTack-1)
     */


    /**getPartition说明
     *
     * @param text :map输出的key类型
     * @param phoneFile map输出的value类型
     * @param i ：程序设置的reduce的数量
     * @return 分区id
     */
    @Override
    public int getPartition(Text text, PhoneFile phoneFile, int i) {
        int partitions;
        String PhoneHead = text.toString().substring(0, 3);
        if("136".equals(PhoneHead)){
            partitions=0;
        }else if("137".equals(PhoneHead)){
            partitions=1;
        }else if("138".equals(PhoneHead)){
            partitions=2;
        }else if("139".equals(PhoneHead)){
            partitions=3;
        }else {
            partitions=4;
        }
        return partitions;
    }
}
