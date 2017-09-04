package com.shi.bigdata.temp_analyse_hadoop.count_by_city_month;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MyValue implements Writable{
	private Integer max;
	private Integer min;
	private Integer diff;
	private Integer avg;
	public Integer getMax() {
		return max;
	}
	public void setMax(Integer max) {
		this.max = max;
	}
	public Integer getMin() {
		return min;
	}
	public void setMin(Integer min) {
		this.min = min;
	}
	public Integer getDiff() {
		return diff;
	}
	public void setDiff(Integer diff) {
		this.diff = diff;
	}
	public Integer getAvg() {
		return avg;
	}
	public void setAvg(Integer avg) {
		this.avg = avg;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(max);
		out.writeInt(min);
		out.writeInt(diff);
		out.writeInt(avg);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		max=in.readInt();
		min=in.readInt();
		diff=in.readInt();
		avg=in.readInt();
		
	}
	
}
