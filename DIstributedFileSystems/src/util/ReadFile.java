

package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.Callable;

public class ReadFile implements Callable {

    private String directoryPath;
    private String fileName;

    public ReadFile(String directoryPath, String fileName)  {
        this.directoryPath = directoryPath;
        this.fileName = fileName;
    }

    public String call()   {

        String lastLine = "";
        try {
            FileReader fileReader = new FileReader(directoryPath + "/" + fileName);

            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = bufferedReader.readLine();
            while (line != null) {
                lastLine+= line;
                line = bufferedReader.readLine();
            }

            bufferedReader.close();
            return lastLine;
        }
        catch(Exception ex) {
            ex.printStackTrace();
            return String.valueOf(ex.getStackTrace());
//            return "Error occurred while reading file " + fileName;
        }
    }
}
