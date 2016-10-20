package code.lemma;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class Tokenizer {

	public static List<String> tokens = null;
	public static List<Pattern> noise = null;

	public static Properties props = null;
	public static StanfordCoreNLP pipeline = null;

	public Tokenizer() {
		tokens = new ArrayList<String>();
		noise = noisePattern();

		props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		pipeline = new StanfordCoreNLP(props);
	}

	public List<String> tokenize(String sentence) {
		
		sentence = noiseFilter(sentence).toLowerCase();

		Annotation document = new Annotation(sentence);
		pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);

		for (CoreMap s: sentences) {
			for (CoreLabel token : s.get(TokensAnnotation.class)) {
				String word = token.get(TextAnnotation.class);
				tokens.add(word);
			}
		}
		return tokens;
	}
	
	public static String noiseFilter(String sentence) {

		for (Pattern p : noise) {
			sentence = p.matcher(sentence).replaceAll(" ");
		}

		return sentence;
	}

	public static List<Pattern> noisePattern() {

		List<Pattern> patterns = new ArrayList<Pattern>();

		//remove 's 'd 't 'll
		Pattern removeS = Pattern.compile("('s|s'|'d|'t|'ll)" + "\\s");
		patterns.add(removeS);
		
		// remove all non-word characters
		Pattern nonword = Pattern.compile("\\P{L}+");
		patterns.add(nonword);
		
		// remove references
		Pattern removeRef = Pattern.compile("(&lt;ref&gt;.+?&lt;/ref&gt;)");
		patterns.add(removeRef);
		
		// remove style markings
		Pattern removeStyle = Pattern.compile("''+");
		patterns.add(removeStyle);
		
		// remove urls
		Pattern removeUrl = Pattern.compile("(https?|http):" + "((//)+[\\w\\/]*)");
		patterns.add(removeUrl);
		
		return patterns;
	}
}

