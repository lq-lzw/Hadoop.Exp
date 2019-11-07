package com.mypro03;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;




public class WordCountCombiner extends Reducer<Text, LongWritable,Text, IntWritable>  {

    protected void reduce(Text k2, Iterable<IntWritable> v2, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable v:v2){
            count += v.get();
            System.out.println("Combiner输入键值对<" + k2.toString() + ","
                    + v.get() + ">");
        }
        context.write(k2,new IntWritable(count));
        System.out.println("combiner over");
    }
}
