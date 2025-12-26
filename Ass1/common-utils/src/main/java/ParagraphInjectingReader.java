import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

public class ParagraphInjectingReader extends Reader {

    private final BufferedReader br;
    private String nextLine;

    public ParagraphInjectingReader(BufferedReader br) {
        this.br = br;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (nextLine == null) {
            nextLine = br.readLine();
            if (nextLine == null) {
                return -1; // end of stream
            }

            // If the line is empty, inject a sentence-break token.
            if (nextLine.trim().isEmpty()) {
                nextLine = ".";
            }

            nextLine += "\n"; // Always re-add newline for tokenizer
        }

        int writeLen = Math.min(len, nextLine.length());
        nextLine.getChars(0, writeLen, cbuf, off);

        if (writeLen == nextLine.length()) {
            nextLine = null;
        } else {
            nextLine = nextLine.substring(writeLen);
        }

        return writeLen;
    }

    @Override
    public void close() throws IOException {
        br.close();
    }
}