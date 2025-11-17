package org.example;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.trees.*;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main2(String[] args) {
        String model = "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz";
        LexicalizedParser lp = LexicalizedParser.loadModel(model);

        String fileName = "gutenberg.org-1659.txt";
        try{
            File txtFile = new File(fileName);
            BufferedReader reader = new BufferedReader(new FileReader(txtFile));
            String line;
            TokenizerFactory<CoreLabel> tokFactory =
                    PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
            while ((line = reader.readLine()) != null) {
                if(line.trim().isEmpty()) continue;

                List<CoreLabel> tokens = tokFactory.getTokenizer(new StringReader(line)).tokenize();

                // 3) Parse
                Tree parse = lp.apply(tokens);

                // 4) Constituency tree
                parse.pennPrint();

                // 5) Typed dependencies
                TreebankLanguagePack tlp = new PennTreebankLanguagePack();
                GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
                GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
                Collection<TypedDependency> tdl = gs.typedDependenciesCCprocessed();
                System.out.println(tdl);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}