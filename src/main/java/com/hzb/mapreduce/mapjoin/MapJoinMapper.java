package com.hzb.mapreduce.mapjoin;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;


/**
 * @author huzhibin
 * @date 2022-07-01 15:46
 */
public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private HashMap<String,String> pdmap= new HashMap<>();
    private  Text outK = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存的文件
        URI[] files = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream cache = fileSystem.open(new Path(files[0]));

        InputStreamReader isr = new InputStreamReader(cache);
        BufferedReader bR = new BufferedReader(isr);

        String line;
        while (StringUtils.isNotEmpty(line = bR.readLine())){
            String[] split = line.split("\t");
            pdmap.put(split[0],split[1]);

        }

        IOUtils.closeStream(bR);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        String pname= pdmap.get(split[1]);
        outK.set(split[0]+"\t"+pname+"\t"+split[2]);
        context.write(outK,NullWritable.get());


    }
}
