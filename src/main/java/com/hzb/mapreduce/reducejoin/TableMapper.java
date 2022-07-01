package com.hzb.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 14:18
 */
public class TableMapper extends Mapper<LongWritable,Text, Text,BeanTable> {
    private  String filename;
    private BeanTable bt=new BeanTable();
    private Text outkey= new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化认为时,通过split,即切片获取当前处理的数据来自那个文件
        InputSplit split = context.getInputSplit();//InputSplit是一个抽象类
        FileSplit fileSplit = (FileSplit) split;//FileSplit是InputSplit的子类
        filename = fileSplit.getPath().getName();//获取切片的文件名

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//这里的逻辑就是,判断当前处理的切片是那个文件的,并按照文件内容将数据封装到自定义类对象的属性中并写出
        if(filename.contains("order")){
            String[] strings = value.toString().split("\t");
                outkey.set(strings[1]);
                bt.setId(strings[0]);
                bt.setPid(strings[1]);
                bt.setAmount(Integer.parseInt(strings[2]));
                bt.setPname("");
                bt.setFilename("order");

        }else {
            String[] strings = value.toString().split("\t");
            outkey.set(strings[0]);
            bt.setId("");
            bt.setPid(strings[0]);
            bt.setAmount(0);
            bt.setPname(strings[1]);
            bt.setFilename("pd");

        }
        context.write(outkey,bt);

    }
}
