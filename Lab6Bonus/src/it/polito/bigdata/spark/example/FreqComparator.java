package it.polito.bigdata.spark.example;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class FreqComparator implements Comparator<Tuple2<Integer, String>>,Serializable {


	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<Integer, String> pair1, Tuple2<Integer, String> pair2) {
			
		return pair1._1().compareTo(pair2._1());
	}

}
