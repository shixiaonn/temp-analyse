package com.shi.bigdata.temp_analyse_hadoop.count_by_city_month;

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
		String[] split = line.split("[\\s-]+");
		//构造MyKey
		MyKey myKey=new MyKey();
		myKey.setCity(split[0]+" "+split[1]);
		myKey.setMonth(split[2]+" "+split[3]);
		//构造MyValue
		MyValue myValue = new MyValue();
		myValue.setMax(Integer.parseInt(split[5]));
		myValue.setMin(Integer.parseInt(split[6]));
		myValue.setDiff(Integer.parseInt(split[7]));
		myValue.setAvg(Integer.parseInt(split[8]));
		context.write(myKey, myValue);
		context.getCounter("count_by_city_month", "mapper").increment(1);
	}

}
