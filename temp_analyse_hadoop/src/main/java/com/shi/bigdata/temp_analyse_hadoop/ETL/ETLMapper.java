package com.shi.bigdata.temp_analyse_hadoop.ETL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 
 * 读取一刚数据,补全完以后输出
 *
 */
public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	//key是城市,value是省份
	private Map<String, String> places = new HashMap<>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 获取第一个缓存文件的文件名
		String fileName = new Path(context.getCacheFiles()[0]).getName();
		// 通过文件名向读取本地文件一样读取缓存文件中的内容
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String tmp = null;
		while((tmp=reader.readLine())!=null){
			String[] split = tmp.split("\\s+");
			places.put(split[1], split[0]);
		}
		reader.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String string = value.toString();
		// 阿城    2011-01-01 -12 -25 晴 西北风~西南风 3-4级~微风
		/*
		 * 怎么获取省份？
		 * 截取城市，通过map获取省份
		 */
		String[] split = string.split("\\s+", 5);
		if(split.length<4){
			context.getCounter("missing", split[0]).increment(1);
			return;
		}
		//获取城市
		String city = split[0];
		//获取省份
		String province=places.get(city);
		//计算温差
		int min = Integer.parseInt(split[3]);
		int max = Integer.parseInt(split[2]);
		int diff =max - min;
		//计算平均温度
		int avg = (max+min)/2;
		
		//最终要输出的格式:
		String result = null;
		if(split.length==4){
			result=province+"\t"+city+"\t"+split[1]+"\t"+max+"\t"+min+"\t"+
					diff+"\t"+avg;
		}else {
			result=province+"\t"+city+"\t"+split[1]+"\t"+max+"\t"+min+"\t"+
					diff+"\t"+avg+"\t"+split[4];
		}
		//写出
				context.write(new Text(result),NullWritable.get());//NullWritable是单例模式
				context.getCounter("etl","map").increment(1);
				
		
		
	}
}
