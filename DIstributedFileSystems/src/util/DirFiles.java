package util;

import java.io.File;
import java.util.concurrent.Callable;

public class DirFiles implements Callable {
	private String dirPath;

    public DirFiles(String path)  {
        this.dirPath = path;
    }

    public String call()   {
	
	try {
        File dir = new File(dirPath);
        File[] listOfFiles = dir.listFiles();

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                sb.append(listOfFiles[i].getName());
                sb.append(",");
            }
            
        }
        return sb.toString();
    }   catch (Exception ex)    {
        ex.printStackTrace();
        return "ERROR";
    }
}
}
