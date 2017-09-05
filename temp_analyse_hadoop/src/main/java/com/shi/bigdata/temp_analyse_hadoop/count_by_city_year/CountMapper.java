package com.shi.bigdata.temp_analyse_hadoop.count_by_city_year;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, MyKey, MyValue>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyKey, MyValue>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		// 上海    上海    2011-01-01      6       2       4       4       多云~雨夹雪 北风 4-5级
		// 上海    上海	  2011-01	   6       2       4       4
		String[] split = line.split("[\\s]+");
		//构造MyKey
		MyKey myKey=new MyKey();
		myKey.setPlace(split[0]+" "+split[1]);
		String[] split2 = split[2].split("-");
		myKey.setDate(split2[0]+" "+split2[1]);
		//构造MyValue
		MyValue myValue = new MyValue();
		myValue.setMax(Integer.parseInt(split[3]));
		myValue.setMin(Integer.parseInt(split[4]));
		myValue.setDiff(Integer.parseInt(split[5]));
		myValue.setAvg(Integer.parseInt(split[6]));
		context.write(myKey, myValue);
		context.getCounter("count_by_city_year", "mapper").increment(1);
	}

}
