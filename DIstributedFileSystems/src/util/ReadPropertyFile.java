
package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReadPropertyFile {

    public static Properties readProperties(String filePath)   {

        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream("config.properties");

            // load a properties file
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return prop;
        }
    }

    public static void main(String[] args)  {

        Properties prop = readProperties("config.properties");
        System.out.println("serverPort: " + prop.getProperty("serverPort"));
        System.out.println("clientPort: " + prop.getProperty("clientPort"));

        for(int i = 1; i <= Integer.valueOf(prop.getProperty("noofserver")); i++)
            System.out.println("serverPort: " + prop.getProperty("server"+i));

        for(int i = 1; i <= Integer.valueOf(prop.getProperty("noofclient")); i++)
            System.out.println("serverPort: " + prop.getProperty("client"+i));

    }
}
