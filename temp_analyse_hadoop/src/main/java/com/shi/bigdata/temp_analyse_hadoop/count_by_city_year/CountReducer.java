package com.shi.bigdata.temp_analyse_hadoop.count_by_city_year;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *聚合函数
 *同一个城市同一个月份的数据聚合到了一块
 *
 *
 *按照城市和月份进行聚合统计（最高气温，最低气温，平均气温，最高温差，最低温差，平均温差）
 *
 *上海    上海 2011 01   6   2    4   6   7   6
 *
 *
 *
 */

public class CountReducer extends Reducer<MyKey, MyValue, Text, NullWritable>{
	@Override
	protected void reduce(MyKey arg0, Iterable<MyValue> arg1, Reducer<MyKey, MyValue, Text, NullWritable>.Context arg2)
			throws IOException, InterruptedException {
		// 求最高气温
		int max = Integer.MIN_VALUE;
		// 求最低气温
		int min = Integer.MAX_VALUE;
		// 平均气温
		int sum = 0;
		int count = 0;
		//求最高温差
		int diff_max=Integer.MIN_VALUE;
		//求最低温差
		int diff_min=Integer.MAX_VALUE;
		//求平均温差
		int diff_sum=0;
		for(MyValue myValue:arg1){
			if(myValue.getMax()>max){
				max=myValue.getMax();
			}
			if(myValue.getMin()<max){
				min=myValue.getMin();
			}
			sum+=myValue.getAvg();
			count++;
			if(myValue.getDiff()>diff_max)
				diff_max=myValue.getDiff();
			if(myValue.getDiff()<diff_min)
				diff_min=myValue.getDiff();
			diff_sum+=myValue.getDiff();
		}
		int avg = sum/count;
		int diff_avg =diff_sum/count;
		String result = arg0.getPlace()+" "+arg0.getDate()+" "+max+" "+min+" "
				+avg+" "+diff_max+" "+diff_min+" "+diff_avg;
		arg2.write(new Text(result), NullWritable.get());
		arg2.getCounter("count_by_city_year", "reducer").increment(1);
	}
}
