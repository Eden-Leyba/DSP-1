//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class TestStanfordParser {

    public static void main(String[] args) throws Exception {
        // 1. Sample text (like a tiny input file)
        String sampleText = "The quick brown fox jumps over the lazy dog. "
                + "This is a second sentence.";

        // 2. Create parser (loads the models)
        StanfordParser parser = new StanfordParser();

        // 3. Test each analysis type
        testMode(parser, sampleText, StanfordParser.AnalysisType.POS);
        testMode(parser, sampleText, StanfordParser.AnalysisType.CONSTITUENCY);
        testMode(parser, sampleText, StanfordParser.AnalysisType.DEPENDENCY);
    }

    private static File downloadWithJava(String urlString) throws Exception {
        System.out.println("Downloading from URL: " + urlString);

        File tempFile = File.createTempFile("web-input-", ".txt");

        try (InputStream in = new URL(urlString).openStream()) {
            Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        System.out.println("Downloaded to: " + tempFile.getAbsolutePath());
        return tempFile;
    }

    private static void testMode(StanfordParser parser,
                                 String text,
                                 StanfordParser.AnalysisType type) throws Exception {

        String url_input = "https://www.gutenberg.org/cache/epub/1065/pg1065.txt"; // example: The Raven
        File inputFile = downloadWithJava(url_input);

        String textBuffer = Files.readString(inputFile.toPath(), StandardCharsets.UTF_8);


        String fileName = "test_output_" + type.name().toLowerCase() + ".txt";
        File outFile = new File(fileName);

        System.out.println("=== Testing mode: " + type + " ===");
        System.out.println("Output file: " + outFile.getAbsolutePath());

        // 1. Run parser for this mode
        try (PrintWriter writer = new PrintWriter(new FileWriter(outFile))) {
            parser.parseTextBuffer(textBuffer, type, writer);
        }

        // 2. Read back content and print first lines to console
        String content = Files.readString(outFile.toPath());
        System.out.println("----- File content (first 300 chars) -----");
        if (content.length() > 300) {
            System.out.println(content.substring(0, 300) + "...");
        } else {
            System.out.println(content);
        }
        System.out.println("==========================================\n");
    }
}