package com.hzb.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-06-29 11:34
 */
public class PhoneReducer  extends Reducer<Text,PhoneFile,Text,Text> {
        Text phoneobjcet= new Text();
        PhoneFile pf = new PhoneFile();
    @Override
    protected void reduce(Text key, Iterable<PhoneFile> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        Long up=0L;
        Long down=0L;
//        Long sum=0L;
        for (PhoneFile value : values) {
           up+=value.getUpFlow();
           down+=value.getDownFlow();
//           sum+=value.getSumFlow();
        }
        pf.setUpFlow(up);
        pf.setDownFlow(down);
        pf.setSumFlow();
        phoneobjcet.set(pf.toString());
        context.write(key,phoneobjcet);

    }
}
