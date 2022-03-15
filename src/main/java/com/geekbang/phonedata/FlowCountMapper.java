package com.geekbang.phonedata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * @author chen.xinliang
 * @create 2022-03-15 8:36
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, IOException {
        String line = value.toString();

        String[] fields = line.split("\t");
        String phoneNum = fields[1];

        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNum);

        context.write(k,new FlowBean(upFlow,downFlow));
    }
}
