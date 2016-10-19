package code.lemma;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import mapreduce.util.StringIntegerList;

/**
 * 
 *
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// TODO: implement Lemma Index mapper here			
			Tokenizer tokenier=new Tokenizer();
			Text title=new Text();
			title.set(page.getTitle());
			List<String> content=tokenier.tokenize(page.getContent());	
			HashMap<String,Integer> word_frequency=new HashMap<String,Integer>();
			for(String lemma:content){
				if(word_frequency.containsKey(lemma)){
					word_frequency.put(lemma,new Integer(word_frequency.get(lemma).intValue()+1));
				}
				else{
					word_frequency.put(lemma, new Integer(1));
				}			
			}			
			StringIntegerList stringIntegerList=new StringIntegerList(word_frequency);		
			context.write(title,stringIntegerList);				
		}
	}
}

