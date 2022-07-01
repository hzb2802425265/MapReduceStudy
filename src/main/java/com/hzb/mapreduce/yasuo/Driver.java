package com.hzb.mapreduce.yasuo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-28 17:05
 */
public class Driver  {
    /**MR的压缩,主要可以在三个地方设置:
     * 1:map前即InputFormat时,这里的压缩设置,需要考虑输入文件的大小,如果大于块大小则要优先考虑压缩的格式是否支持切片,如果小于块大小则有限考虑压缩二号解压速度
     * 这里常用的LZO(),Snappy,处理小与块大小的文件压缩,常使用bzip2,LZO压缩需要切片的大文件
     * 2:map结束输出时:这里不需要考虑切片问题,有限考虑压缩速度和效率,一般采用LZO,Snappy
     * 3:reduce结束输出时,这里要考虑,结果文件是否作为了其他mr的前置依赖,如果是则和map前的选择压缩策略一致,如果不是则优先考虑压缩效率,常用Bzip2和Gzip
     *
     * 几种压缩方式:
     *格式                             压缩后缀         支持切片   程序是否需要修改
     * DEFLATE	hadoop自带，直接使用	DEFLATE	.deflate	否	和文本处理一样，不需要修改
     * Gzip	    hadoop自带，直接使用	DEFLATE	.gz	        否	和文本处理一样，不需要修改
     * bzip2	hadoop自带，直接使用	bzip2	.bz2	    是	和文本处理一样，不需要修改
     * LZO	    需要安装   ，需要安装	LZO	.lzo	        是	需要建索引，还需要指定输入格式
     * Snappy	hadoop自带，直接使用	Snappy	.snappy	    否	和文本处理一样，不需要修改
     * 各个特点
     * Gzip压缩
     * 优点：压缩率比较高；
     * 缺点：不支持Split；压缩/解压速度一般；
     *  Bzip2压缩
     * 优点：压缩率高；支持Split；
     * 缺点：压缩/解压速度慢。
     *  Lzo压缩
     * 优点：压缩/解压速度比较快；支持Split；
     * 缺点：压缩率一般；想支持切片需要额外创建索引。
     * Snappy压缩
     * 优点：压缩和解压缩速度快；
     * 缺点：不支持Split；压缩率一般；
     *
     *
     * 输入时的解压,hadoop会根据文件后缀名完成对于解码器的使用
     * 如果需要添加编码解码的支持,配置core-site.xml文件的io.compression.codecs//默认时无参数
     *
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//      1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
//############################MAP处的压缩设置方式##########################################
        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);

        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
//        conf.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);
//        conf.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, CompressionCodec.class);
//######################################################################
        Job job = Job.getInstance(conf);
//      2 关联本Driver程序的jar (主程序类)
        job.setJarByClass(Driver.class);
//      3 关联Mapper和Reducer的jar(程序类)
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
//      4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//      5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//###############################reduce处的压缩设置方式#######################################
        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
// ######################################################################

//      6 设置job的输入和输出路径
        //win本地路径
        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\input"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\outWord"));
        //打jar包在Linux中执行自己输入路径
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//      7 提交job
        boolean result = job.waitForCompletion(true);//提交job，true查看完整日志
        System.exit(result ? 0 : 1);//退出

    }
}
