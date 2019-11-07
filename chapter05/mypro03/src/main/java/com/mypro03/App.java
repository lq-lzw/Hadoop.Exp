package com.mypro03;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
        Job job = Job.getInstance(new Configuration());
        //main函数所在的class
        job.setJarByClass(App.class);
        //指定job和mapper和输出的类型《k1,v1>
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);//k1的类型
        job.setMapOutputValueClass(IntWritable.class);//v1类型
        //指定combiner
        job.setCombinerClass(WordCountCombiner.class);
//        job.setMapOutputKeyClass(Text.class);//k2的类型
//        job.setMapOutputValueClass(IntWritable.class);//v2类型
        //指定job和reducer的输出类型k3 v3
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定job输入输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //执行
        job.waitForCompletion(true);
        System.out.println( "Hello World!" );
    }
}
