package benchtpce.parser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Created by carlosmorais on 23/11/2016.
 */
public final class FixXML {
    public static void fixFiles(String filename) {
        Path pathRead = Paths.get(System.getProperty("user.dir") + "/src/main/resources/files/original/" + filename);
        Path pathWrite = Paths.get(System.getProperty("user.dir") + "/src/main/resources/files/fixes/" + filename);

        boolean flag = false;
        try( Stream<String> lines = Files.lines(pathRead) ){
            BufferedWriter writer = Files.newBufferedWriter(pathWrite);

            for( String line : (Iterable<String>) lines::iterator ) {
                if(line.matches("[ \\t\\.]*<writeset>(\\.)*"))
                    flag = true;
                else if(line.matches("[ \\t\\.]*<\\/writeset>(\\.)*"))
                    flag = false;
                else if(flag && (line.matches("[ \\t\\.]*<statement>(.*?)") || line.matches("[ \\t]*(\\.)*<transaction_id>(.*?)")))
                    writer.write("\t</writeset>\n"); //fix the end tag!

                //the benchtpce.parser read the file as XML, in XML '&' must be '&amp;'
                line = line.replaceAll("&", "&amp;");

                //write the current line to the new file
                writer.write(line+"\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
