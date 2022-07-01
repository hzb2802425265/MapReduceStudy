package com.hzb.mapreduce.mapjoin;


import com.hzb.mapreduce.reducejoin.TableMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author huzhibin
 * @date 2022-07-01 15:41
 */
public class Driver {
    /**map join的说明
     * 在map进行join的核心思想是,先将一张表/文件读取到内存中,然后map()输入另一张表/文件,并和在内存中的数据根据join的主键进行关联,以此来实现mapjoin
     * 这种方式往往适合数量两张表/文件,一大一小,可以将小的在setup()初始化时就加载到内存中,对表数据进行读取,案例中时放在hashmap的集合中.也可以对象集合中.
     * 而map()里则是对大文件进行处理,根据主键进行集合间的操作,以此完成join,这样可以就不要在reduce出进行join的逻辑处理了.
     *
     *
     *
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(com.hzb.mapreduce.reducejoin.Driver.class);
        job.setMapperClass(MapJoinMapper.class);
//        job.setReducerClass(TableReduer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        ############################################################################
        //指定提前缓存的文件uri
        job.addCacheFile((new URI("file:///E:/JavaCode/testTxt/inputtable/pd.txt")));
        job.setNumReduceTasks(0);//仅仅join在map出就完成了,不需要reduce了
//        ############################################################################
        FileInputFormat.setInputPaths(job,new Path("E:\\JavaCode\\testTxt\\inputtable2"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\JavaCode\\testTxt\\outtable2"));
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);



    }
}
