package edu.umd.clip.mt.alignment;

import edu.umd.clip.mt.Alignment;

public abstract class Refiner {
	public abstract Alignment refine(Alignment a1, Alignment a2);
}
