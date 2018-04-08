
import util.ReadPropertyFile;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        if(args.length == 2) {
            try {
            	Properties prop = ReadPropertyFile.readProperties("config.properties");
            	String SystemType = args[0];
                String SystemID = args[1];

                if (SystemType.equalsIgnoreCase("client")) {
                    Client client = new Client(prop, SystemID);
                    client.startClient();
                }
                else if (SystemType.equalsIgnoreCase("metaserver")){
                    MetaServer server = new MetaServer(prop, SystemID);
                    server.startServer();
                }
                else if (SystemType.equalsIgnoreCase("server")){
                    Server server = new Server(prop, SystemID);
                    server.startServer();
                }
                else {
                    System.out.println("Invalid Input");
                }
            }
            catch (Exception ex)    {
                ex.printStackTrace();
            }
        }
        else    {
            System.out.println("Invalid Input");
            System.out.println("Run Configuration: <server/metaserver/client> <id>");
        }
    }
}
