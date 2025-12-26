

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.trees.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class StanfordParser {

    public enum AnalysisType {
        POS,
        CONSTITUENCY,
        DEPENDENCY
    }

    // Not static – per parser instance
    private final LexicalizedParser lp;
    private final GrammaticalStructureFactory gsf;

    public StanfordParser() {
        System.out.println("Loading models... (This takes a few seconds)");

        String modelPath = "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz";
        this.lp = LexicalizedParser.loadModel(modelPath);

        TreebankLanguagePack tlp = new PennTreebankLanguagePack();
        this.gsf = tlp.grammaticalStructureFactory();

        System.out.println("Models loaded. Ready to parse.");
    }

    public int getNumSentences(File file_to_analyze) throws FileNotFoundException {
        if (file_to_analyze == null ) {
            return -1;
        }

        List<List<HasWord>> sentences = initTokenizer(file_to_analyze);

        int sentenceId = 1;
        for (List<HasWord> _ : sentences) {
            sentenceId++;
        }

        return sentenceId;
    }

    private void parseSentence(List<HasWord> sentence, AnalysisType analysisType, PrintWriter writer) {
        Tree parse = lp.apply(sentence);
        switch (analysisType) {
            case POS:
                writePOS(parse, writer);
                break;
            case CONSTITUENCY:
                writeConstituency(parse, writer);
                break;
            case DEPENDENCY:
                writeDependencies(parse, writer);
                break;
        }
        writer.println();
        writer.flush();
    }

    private DocumentPreprocessor initDocumentPreprocessor(File file_to_analyze) throws FileNotFoundException {
        if (file_to_analyze == null ) {
            return null;
        }

        BufferedReader br = new BufferedReader(new FileReader(file_to_analyze));
        ParagraphInjectingReader pir = new ParagraphInjectingReader(br);
        PushbackReader pushbackReader = new PushbackReader(pir, 8192);

        return new DocumentPreprocessor(pir);
    }

    public List<List<HasWord>> initTokenizer(File file_to_analyze) throws FileNotFoundException {
        DocumentPreprocessor tokenizer = initDocumentPreprocessor(file_to_analyze);

        tokenizer.setSentenceFinalPuncWords(new String[]{".", "?", "!", "!!", "!!!", "??", "?!", "!?", ";"});

        List<List<HasWord>> result = new ArrayList<>();

        int sentenceId = 1;
        for (List<HasWord> sentence : tokenizer) {
            result.add(sentence);
            sentenceId++;
        }

        return result;
    }

    public void parseTextBuffer(List<List<HasWord>> sentences, AnalysisType analysisType, PrintWriter writer, int sentence_start_idx, int sentence_end_idx) {

        int sentenceId = sentence_start_idx;
        for(List<HasWord> sentence : sentences) {
            if(sentenceId > sentence_end_idx) {
                break;
            }
            if (sentence == null || sentence.isEmpty()) {
                System.out.println("Sentence " + sentenceId);
                writer.println("--- Sentence " + sentenceId + " ---");
                sentenceId++;
                continue;
            }
            System.out.println("Sentence " + sentenceId);
            writer.println("--- Sentence " + sentenceId + " ---");
            parseSentence(sentence, analysisType, writer);
            sentenceId++;
        }
    }


    private void writePOS(Tree parse, PrintWriter writer) {
        writer.print("[POS]: ");
        ArrayList<TaggedWord> posTags = parse.taggedYield();
        for (TaggedWord tw : posTags) {
            writer.print(tw.word() + "/" + tw.tag() + " ");
        }
        writer.println();
    }

    private void writeConstituency(Tree parse, PrintWriter writer) {
        writer.println("[Constituency]:");
        parse.pennPrint(writer);  // writes directly into our PrintWriter
    }

    private void writeDependencies(Tree parse, PrintWriter writer) {
        writer.println("[Dependency]:");
        GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
        Collection<TypedDependency> tdl = gs.typedDependenciesCCprocessed();
        writer.println(tdl);
    }
}

