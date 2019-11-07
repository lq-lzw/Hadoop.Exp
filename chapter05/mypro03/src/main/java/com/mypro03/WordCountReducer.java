package com.mypro03;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {


    @Override
    protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
        //contex代表上下文  //
        int total = 0;
        for(IntWritable v:v3){
            total += v.get();
            System.out.println("Reducer输入键值对<" + k3.toString() + ","
                    + v.get() + ">");
        }
        context.write(k3,new IntWritable(total));
        System.out.println("reduce over!");
    }
}
