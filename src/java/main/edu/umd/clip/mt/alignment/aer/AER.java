package edu.umd.clip.mt.alignment.aer;

import java.util.Iterator;

import edu.umd.clip.mt.Alignment;

public class AER {

	public static float computeAER(Iterator<ReferenceAlignment> refi, Iterator<Alignment> testi) {
		float num = 0.0f;
		float den = 0.0f;
		while (refi.hasNext()) {
			ReferenceAlignment ref = refi.next();
			Alignment test = testi.next();
			num += (ref.countProbableHits(test) + ref.countSureHits(test));
			den += (test.countAlignmentPoints() + ref.countAlignmentPoints());
		}
		if (testi.hasNext())
			throw new RuntimeException("Mismatch in lengths");
		return num / den;
	}
}
