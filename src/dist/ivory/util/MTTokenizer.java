package ivory.util;

public class MTTokenizer implements Tokenizer {

	public String[] processContent(String text) {
		int length = 4;
		String[] arg = text.split("\\s+");
		String[] res = new String[arg.length];
		for (int i =0; i < arg.length; ++i) {
			final String cur = arg[i].toLowerCase();
			int l = length;
			if (cur.startsWith("con"))
				l+=2;
			else if (cur.startsWith("intra"))
				l+=4;
			else if (cur.startsWith("pro"))
				l+=2;
			else if (cur.startsWith("anti"))
				l+=3;
			else if (cur.startsWith("inter"))
				l+=4;
			else if (cur.startsWith("in"))
				l+=2;
			else if (cur.startsWith("im"))
				l+=2;
			else if (cur.startsWith("re"))
				l+=2;
			else if (cur.startsWith("de"))
				l+=1;
			else if (cur.startsWith("pre"))
				l+=2;
			else if (cur.startsWith("un"))
				l+=2;
			else if (cur.startsWith("co"))
				l+=2;
			else if (cur.startsWith("qu"))
				l+=1;
			else if (cur.startsWith("ad"))
				l+=1;
			else if (cur.startsWith("en"))
				l+=2;
			else if (cur.startsWith("al-"))
				l+=2;
			else if (cur.startsWith("sim"))
				l+=2;
			else if (cur.startsWith("sym"))
				l+=2;
			if (cur.length() < l) l = cur.length();
			res[i] = cur.substring(0, l);
		}
		return res;

	}

}
