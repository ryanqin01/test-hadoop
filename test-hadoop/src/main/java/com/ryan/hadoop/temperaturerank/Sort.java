package com.ryan.hadoop.temperaturerank;

import org.apache.hadoop.io.WritableComparator;

public class Sort extends WritableComparator {

	public Sort() {
		super(KeyPair.class, true);
	}

	@Override
	public int compare(Object a, Object b) {

		KeyPair k1 = (KeyPair) a;
		KeyPair k2 = (KeyPair) b;

		int iRet = Integer.compare(k1.getYear(), k2.getYear());
		if (iRet != 0) {
			return iRet;
		}
		return Integer.compare(k2.getTemperature(), k1.getTemperature());
	}
}