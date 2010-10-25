package ivory.smrf.model.builder;

public class Expression {

	public static enum Type {
		OD, UW, TERM
	}
	
	private final Type mType;
	private final String[] mTerms;
	private final int mWindow;
	
	public Expression(Type type, int window, String[] terms) {
		mType = type;
		mWindow = window;
		mTerms = terms;
	}
	
	public Expression(String term) {
		mType = Type.TERM;
		mWindow = -1;
		mTerms = new String[] { term };
	}
	
	public String[] getTerms() {
		return mTerms;
	}
	
	public int getWindow() {
		return mWindow;
	}
	
	public Type getType() {
		return mType;
	}	
}
