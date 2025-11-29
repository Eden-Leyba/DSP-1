import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class TestCountingSentences {
    private static File downloadWithJava(String urlString) throws Exception {
        System.out.println("Downloading from URL: " + urlString);

        File tempFile = File.createTempFile("web-input-", ".txt");

        try (InputStream in = new URL(urlString).openStream()) {
            Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        System.out.println("Downloaded to: " + tempFile.getAbsolutePath());
        return tempFile;
    }

    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();
        String input_file_to_analyze_url = "https://www.gutenberg.org/files/1660/1660-0.txt";
        File input_File_to_analyze = downloadWithJava(input_file_to_analyze_url);

        DocumentPreprocessor tokenizer =
                new DocumentPreprocessor(new BufferedReader(new FileReader(input_File_to_analyze )));

        int sentenceId = 1;
        for (List<HasWord> sentence : tokenizer) {
            sentenceId++;
        }

        long end = System.nanoTime();
        long durationNs = end - start;
        System.out.println(sentenceId);
        System.out.println("Execution time: " + (durationNs / 1_000_000.0) + " ms");
    }
}
