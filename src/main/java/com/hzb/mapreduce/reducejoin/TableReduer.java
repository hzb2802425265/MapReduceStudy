package com.hzb.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author huzhibin
 * @date 2022-07-01 14:36
 */
public class TableReduer  extends Reducer<Text,BeanTable,BeanTable,NullWritable> {
//这一部分的逻辑是,当前处理的数据肯定是key相同的,那么value是自定义类的对象,根据对象中的标识符判断执行那种逻辑
    @Override
    protected void reduce(Text key, Iterable<BeanTable> values, Context context) throws IOException, InterruptedException {
//
        ArrayList<BeanTable> list =  new ArrayList<>();
        BeanTable pdbt = new BeanTable();
        for (BeanTable value : values) {
            if("order".equals(value.getFilename())){
                //hadoop对迭代器的优化是,直接地址添加,因此只有一个对象value,直接传入会出现后一个对象覆盖前一个的问题
//              因此这里采用创建一个新的对象,并copy对象的属性后再添加到集合中.
                BeanTable order = new BeanTable();
                try {
                    BeanUtils.copyProperties(order,value);//复制对象 value的所有属性都会复制到 order中
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                list.add(order);
            }else {

                try {
                    BeanUtils.copyProperties(pdbt,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }

        }
        for (BeanTable table : list) {
            //这里执行的就是案例join的操作
            table.setPname(pdbt.getPname());
            //输出当前对象,会按照重写的toString方法写入输出文件
            context.write(table,NullWritable.get());

        }

    }
}
