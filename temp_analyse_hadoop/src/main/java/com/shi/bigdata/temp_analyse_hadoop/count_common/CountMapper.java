package com.shi.bigdata.temp_analyse_hadoop.count_common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, MyKey, MyValue>{
	private String place;
	private String date;
	@Override
	protected void setup(Mapper<LongWritable, Text, MyKey, MyValue>.Context context)
			throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();
		place = conf.get("count.place", "province");
		date = conf.get("count.date", "year");
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyKey, MyValue>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		// 上海    上海    2011-01-01      6       2       4       4       多云~雨夹雪 北风 4-5级
		// 上海    上海	  2011-01	   6       2       4       4
		String[] split = line.split("[\\s]+");
		//构造MyKey
		MyKey myKey=new MyKey();
		if("province".equals(place)){
			myKey.setPlace(split[0]);			
		}else {
			myKey.setPlace(split[0]+" "+split[1]);						
		}
		String[] split2 = split[2].split("-");
		if("year".equals(date)){
			myKey.setDate(split2[0]);			
		}else {
			myKey.setDate(split2[0]+" "+split2[1]);			
		}
		//构造MyValue
		MyValue myValue = new MyValue();
		myValue.setMax(Integer.parseInt(split[3]));
		myValue.setMin(Integer.parseInt(split[4]));
		myValue.setDiff(Integer.parseInt(split[5]));
		myValue.setAvg(Integer.parseInt(split[6]));
		context.write(myKey, myValue);
		context.getCounter("count_by_"+place+"_"+date, "mapper").increment(1);
	}

}
