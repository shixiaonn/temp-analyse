package com.shi.bigdata.temp_analyse_hadoop.count_by_city_month;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * 自定义key
 * 重写序列化方法,反序列化方法,排序方法
 * 
 *
 */
public class MyKey implements WritableComparable<MyKey>{
	private String city;
	private String month;
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getMonth() {
		return month;
	}
	public void setMonth(String month) {
		this.month = month;
	}
	/*
	 * 序列化方法,按照定义的顺序写
	 * 
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(city);
		out.writeUTF(month);
		
	}
	/*
	 *  反序列化方法,按照定义的顺序写
	 * 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		city = in.readUTF();
		month= in.readUTF();
		
	}
	/*
	 * 排序方法 
	 * 排序会影响分组
	 *
	 */
	@Override
	public int compareTo(MyKey o) {
		String result = city+"\t"+month;
		String result2 = o.getCity()+"\t"+o.getMonth();
		return result.compareTo(result2);
	}
	
	/*
	 * 默认分区通过key的hashCode得出来的相等
	 * HashPartitioner调用了HashCode方法
	 * 而默认的HashCode方法是object的HashCode方法
	 * 故而要重写HashCode方法
	 * 
	 */
	
	@Override
	public int hashCode() {
		return (city+"/t"+month).hashCode();
	}

}
