import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinPages {
	public static String searchTitle(String text) {
		String title = "";
		if(text.length() >= 11 && text.substring(0,11).equals("    <title>")) {
			title = text.substring(11, text.length() - 8);
		}
		return title;
	}
	
	public static String searchUrl(String bd, String title) {
		String url = title;

		if(url != "")
		{
			url = url.replaceAll(" ", "_");
			url = "https://" + bd + ".wikipedia.org/wiki/" + url;
		}
		return url;
	}
	
	public static String searchContent(String text) {
		String content = "";

		if (text.length() >= 40 && text.substring(0,11).equals("      <text"))
			content = text.substring(39, text.length());
		else if (text.length()>=2 && !text.substring(0,1).equals(" ") && !text.substring(0,2).equals("</") && !text.substring(0,1).equals("<"))
			content = text;

		return content;
	}
	
	private static boolean isalnum(char c) {
	    return (c >= 'a' && c <= 'z') ||
	           (c >= 'A' && c <= 'Z') ||
	           (c >= '0' && c <= '9');
	}
	
	public static boolean isEnglish(Page p) {
		return (p.getBD().equals("enwiki"));
	}
	
	public static Vector<String> extractEngKeyword(String content) {
		Vector<String> wordList = new Vector<String>();
		String word = "";

		char letter;
		int size = content.length() - 1;

		for(int i=0; i<size; i++) {
			letter = content.charAt(i);

			if(isalnum(letter)) {
				word += letter;
			}
			else {
				if(word != "") {
					if(letter == ' ' || letter == '-') {
						if(isalnum(content.charAt(i+1))) {
							word += letter;
						}
					}
					else {
						wordList.addElement(word);
						word = "";
					}
				}
			}
		}
		return wordList;
	}
	
	public static void write(String title, String koUrl, String word, String enUrl) {
		System.out.println("\n===== CREATE HIPERLINK =====");
		System.out.println("> Word: " + word + " == Title: " + title);
		System.out.println("> Korean page: " + koUrl);
		System.out.println("> English page: " + enUrl);
	}
	
	public static class KoreanMapper
	extends Mapper<Object, Text, Text, Text> {
		private String title = "";
		
		public void map(Object key, Text input, Context context)
			throws IOException, InterruptedException {
			String line = input.toString();
			String content = searchContent(line);
			Vector<String> wordList = new Vector<String>();
			
			title = searchTitle(line);
			content = searchContent(line);
			wordList = extractEngKeyword(content);
			
			int size = wordList.size();
			
			Page inp = new Page("kowiki", title, searchUrl("ko", title));
			for(int i=0; i<size; i++) {
				System.out.println("KoMapWord => " + wordList.elementAt(i));
				context.write(new Text(wordList.elementAt(i)), new Text(inp.serializePage()));
			}
		}
	}
	
	public static class EnglishMapper 
	extends Mapper<Object, Text, Text, Text> {
		private String title = "";
		private String url = "";
		
		public void map(Object key, Text input, Context context)
			throws IOException, InterruptedException {
            String line = input.toString();
            
            title = searchTitle(line);
			url = searchUrl("en", title.toString());
			
			if(!title.equals("")) {
				Page out = new Page("enwiki", title, url);
				System.out.println("EnMapTitle => " + title);
				context.write(new Text(title), new Text(out.serializePage()));
			}
		}
	}
	
	public static class PagesReducer 
	extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			Vector<Page> list = new Vector<Page>();
			String word = "";
			String url = "";
						
			for(Text t : values) {
				Page p = new Page(t.toString());
			
				if(isEnglish(p)) {
					word = p.getTitle();
					url = p.getUrl();
				}
				else
					list.addElement(p);
			}
			
			if(!word.equals("")) {
				for(Page koreanPage : list) {
					write(key.toString(), koreanPage.getUrl(), word, url);
					context.write(key, new Text(koreanPage.getUrl()));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "join pages");
	    
	    job.setJarByClass(JoinPages.class);
	    job.setMapperClass(EnglishMapper.class);
	    job.setMapperClass(KoreanMapper.class);
	    job.setReducerClass(PagesReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    // Delete output file if it exists
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(new Path(args[2]))) {
	       fs.delete(new Path(args[2]), true);
	    }

	    // Each path per mapper
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, EnglishMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, KoreanMapper.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}