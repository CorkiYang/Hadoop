package com.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

/**
 * 用來描述一個特定作業
 * 比如,該作業使用哪個類作為邏輯處理中的map,哪個作為reduce
 * 還可以指令該作業要處裡的數據所在的路徑
 * 還可以指定改作業輸出的結果放到哪個路徑
 * @author user
 *
 */
public class WCRunner {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
		Job wcjob = Job.getInstance(conf);
		wcjob.setJarByClass(WCRunner.class);
		
		wcjob.setMapperClass(WCMapper.class);
		wcjob.setReducerClass(WCReducer.class);
		
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);
		
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(wcjob, new Path("/wc/srcdata/"));
		
		FileOutputFormat.setOutputPath(wcjob, new Path("/wc/output/"));
		
		wcjob.waitForCompletion(true);
	}

}
