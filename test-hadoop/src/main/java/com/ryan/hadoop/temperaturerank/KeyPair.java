package com.ryan.hadoop.temperaturerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyPair implements WritableComparable<KeyPair> {

	private int year;
	private int temperature;

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.temperature = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(temperature);
	}

	@Override
	public int compareTo(KeyPair o) {
		int iRet = Integer.compare(year, o.getYear());
		if (iRet != 0) {
			return iRet;
		}
		return Integer.compare(temperature, o.getTemperature());
	}

	@Override
	public String toString() {
		return year + "\t" + temperature;
	}

	@Override
	public int hashCode() {
		return new Integer(year + temperature).hashCode();
	}
}