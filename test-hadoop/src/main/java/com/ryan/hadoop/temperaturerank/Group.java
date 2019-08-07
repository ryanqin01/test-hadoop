package com.ryan.hadoop.temperaturerank;

import org.apache.hadoop.io.WritableComparator;

public class Group extends WritableComparator {

	public Group() {
		super(KeyPair.class, true);
	}

	@Override
	public int compare(Object a, Object b) {
		KeyPair k1 = (KeyPair) a;
		KeyPair k2 = (KeyPair) b;
		return Integer.compare(k1.getYear(), k2.getYear());
	}
}