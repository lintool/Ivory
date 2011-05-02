package edu.umd.clip.mt.alignment;

import edu.umd.clip.mt.PhrasePair;

public interface AlignmentEventListener {

	public void notifyUnalignablePair(PhrasePair pp, String reason);

}
