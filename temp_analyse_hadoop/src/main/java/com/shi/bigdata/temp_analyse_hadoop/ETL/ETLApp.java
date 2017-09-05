package com.shi.bigdata.temp_analyse_hadoop.ETL;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 *ETL任务:e:extract;t:transaction;l:load 
 *数据清洗,数据转换,数据补全
 * 
 * 把每个月每个城市的气温数据读取出来
 * 过滤掉不符合要求的数据(数据项不全)
 * 通过分布式缓存文件(城市和省份的映射表)将省份数据补全
 * 通过每月的最高气温最低气温计算出平均温度和温差,并将数据补全
 * 最终的结果输出供后续的数据分析,任务执行
 * 
 */


/**
 * 作用：补全数据
 * 阿城    2011-01-01 -12 -25 晴 西北风~西南风 3-4级~微风
 * 
 * 黑龙江	阿城    2011-01-01 -12 -25	13 -18	 晴 西北风~西南风 3-4级~微风
 *
 *
 *mr任务
 *no reduce
 *NullWritable
 *分布式缓存
 *计数器
 *etl
 *
 *输入是：temp-thread-1.log
 *缓存文件(映射文件)是：area.log
 *
 */
public class ETLApp {
	public static void main(String[] args) throws  Exception {
		Job job = Job.getInstance();
		job.setJobName("etl");
		job.setJarByClass(ETLApp.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(ETLMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//添加缓存文件到hadoop分布式缓存中
		
		job.addCacheFile(new Path(args[2]).toUri());
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.out.println(waitForCompletion?0:1);
		
	}
}
