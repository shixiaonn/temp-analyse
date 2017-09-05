package com.shi.bigdata.temp_analyse_hadoop.count_common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 聚合任务:
 * 将上一步etl任务处理后的结果进行聚合
 * 按照城市和月份的维度进行聚合,计算出每一个城市每一个月份的最高最低平均温度和温差
 * 将温度大数据
 * 
 * 
 * 
 */

/**
 *  hadoop jar *.jar 包名.类名   输入  输出 
 *
 *  hadoop jar *.jar 包名.类名   -Dcount.place=city  -Dcount.date=year
 *  
 *  
 */
public class CountApp {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//-Dcount.place=city  -Dcount.date=year   ./in   ./out
		//将其命令行的参数提取出来-D开头的参数,将参数的值反映给conf
		//将其与参数保存起来
		
		GenericOptionsParser genericOptionsParser =new GenericOptionsParser(conf, args);
		String[] remainingArgs = genericOptionsParser.getRemainingArgs();
		
		conf.get("count.place","province");
		conf.get("count.date","year");
		Job job = Job.getInstance();
		
		//设置任务名,任务类
		job.setJobName("count_by_city_year");
		job.setJarByClass(CountApp.class);
		
		//设置输入
		TextInputFormat.setInputPaths(job, new Path(remainingArgs[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		//设置map
		job.setMapperClass(CountMapper.class);
		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(MyValue.class);
		
		//设置reduce
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(3);
		
		//设置输出
		TextOutputFormat.setOutputPath(job, new Path(remainingArgs[1]+"/"+System.currentTimeMillis()));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 提交任务，等待任务结束，打印统计信息
		// 正常结束返回true，非正常结束返回false
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
