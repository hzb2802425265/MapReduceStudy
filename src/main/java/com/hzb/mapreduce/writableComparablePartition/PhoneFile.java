package com.hzb.mapreduce.writableComparablePartition;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-30 22:00
 */
public class PhoneFile implements WritableComparable<PhoneFile> {
    /***序列化的说明
     * 在MR程序中，对一个类的对象进行传输，就需要进行序列化，这和JAVA类似。类必须支持序列化。
     * 在hadoop中的类需要实现Writable，并重新他的序列化和反序列化方法，且顺序要一致
     * 实现序列化的类必须提供一个空参构造器
     * 如果需要将对象写入磁盘文件，可以通过重写toString()方法实现写入逻辑
     * 如果对象类型，作为key传输使用则必须要实现Comparable接口，因为MapReduce框中的Shuffle过程要求对key必须能排序，
     *
     * 当类需要作为key进行传输时，不论是否需要实现排序逻辑，都必须实现WritableCaomparable接口
     *
     */

    private Long downFlow;
    private Long upFlow;
    private Long sumFlow;

    @Override
    public String toString() {
        return "\t" +
                "\t" + downFlow +
                "\t" + upFlow +
                "\t" + sumFlow;
    }

    public PhoneFile() {

    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.downFlow+this.upFlow;
    }

    @Override//序列化方法
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    @Override//反序列化方法
    public void readFields(DataInput dataInput) throws IOException {
       this.upFlow= dataInput.readLong();
       this.downFlow=dataInput.readLong();
       this.sumFlow=dataInput.readLong();

    }

    @Override //重新compareTo方法，实现排序逻辑
    public int compareTo(PhoneFile o) {
        //对sumFLow全排序,其相等时按照upflow排序，即二次排序

        if(this.sumFlow>o.sumFlow){
            return -1;
        }else if (this.sumFlow<o.sumFlow){
            return 1;
        }else {
            if(this.upFlow>o.upFlow){
                return -1;
            }else if (this.upFlow<o.upFlow){
                return 1;
            }else {
                return 0;
            }
        }



    }
}
