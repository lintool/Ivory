package ivory.smrf.model.builder;

public class Expression {
	
	public static enum Type { OD, UW, TERM, XSameBin, XOADJ, XUADJ }
	
	private final Type type;
	private final String[] terms;
	private final int window;
	
	public Expression(Type type, int window, String[] terms) {
		this.type = type;
		this.window = window;
		this.terms = terms;
	}
	
	public Expression(String term) {
		this.type = Type.TERM;
		this.window = -1;
		this.terms = new String[] { term };
	}
	
	public String[] getTerms() {
		return terms;
	}
	
	public int getWindow() {
		return window;
	}
	
	public Type getType() {
		return type;
	}	
}
