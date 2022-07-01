package com.hzb.mapreduce.outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 10:35
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    /**类的说明:
     * 继承了RecordWrite<k, v>这里的k,v也与reduce输出的类型一致
     * 在这个类中具体实现输出了自定义逻辑,主要是在重写的wrtie方法中,close方法中关闭write中使用各种流资源
     */
    //
    private   FSDataOutputStream atguigu;
    private   FSDataOutputStream other;
    //带参构造器,传入的是mr生成的job对象
    public LogRecordWriter(TaskAttemptContext job)
    {

        try {
            //获取job的配置文件信息
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //创建输出路径
            atguigu = fs.create(new Path("E:\\JavaCode\\testTxt\\OutputFormat\\atguigu.log"));
             other = fs.create(new Path("E:\\JavaCode\\testTxt\\OutputFormat\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //具体的写入逻辑
        String line = text.toString();
        if(line.contains("atguigu")){
            atguigu.writeBytes(line+"\n");
        }else {
            other.writeBytes(line+"\n");
        }


    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关闭流
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);

    }
}
