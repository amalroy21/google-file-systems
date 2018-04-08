

package util;

import java.io.*;
import java.util.concurrent.Callable;

public class CreateFile implements Callable {

    private String dir;
    private String fileName;

    public CreateFile(String dir, String fileName)  {
        this.dir = dir;
        this.fileName = fileName;
        
    }

    public Boolean call()   {
        try {
        	System.out.println("Inside Create Filename="+fileName+" Dir-"+dir);
        	int fileID=Integer.parseInt(fileName.replace(".txt", ""));
        	File file = new File(dir + "/" + fileID+"_1.txt");
        	  
        	//Create new chunk
        	if (file.createNewFile()){
        		System.out.println("File is created!");
        		return true;
        	}
        	else{
        	System.out.println("File already exists.");
        	}
        	
        	return false;
        }
        catch(IOException ex) {
            System.out.println("Error writing to file '" + fileName + "'");
            return false;
        }
    }
}
