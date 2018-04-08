import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import util.*;

public class MetaServer {

    private ServerSocket soc;
    private String directoryPath;
    private List<String> serverList = new ArrayList<>();
    private Map<Integer,Boolean> ServerStatus;
    private int serverPort;
    private Properties prop;
    private String clean;

    public MetaServer(Properties properties, String serverID) throws IOException {
        this.soc = new ServerSocket(Integer.valueOf(properties.getProperty("metaserverport")));
        this.directoryPath = properties.getProperty("metaDir");
        //ClientList = new HashMap<>();
        int n = Integer.valueOf(properties.getProperty("no_of_servers"));
        this.prop = properties;
        //System.out.println(properties.getProperty("server"+i));
        for(int i=1;i<=n;i++){
        	this.serverList.add(properties.getProperty("server"+i));
        	System.out.println(properties.getProperty("server" + i));
        }
        this.serverPort = Integer.valueOf(properties.getProperty("serverport"));
        this.clean = properties.getProperty("clean");
    }

    /**
     * Start the server to start listening from clients
     */
    @SuppressWarnings("unchecked")
	public void startServer()   {

        //System.out.println("Starting Server " + ServerID);
        System.out.println("----------------------Meta Server Started---------------------------------");
        System.out.println("Waiting for client on port " +soc.getLocalPort() + "...");
        System.out.println();
        
        if("true".equalsIgnoreCase(clean)){
        	cleanDirectory(directoryPath);
        }
        ExecutorService pool = Executors.newFixedThreadPool(3);
        while(true) {

            try {
                Socket server = soc.accept();
                String clientIP = server.getInetAddress().toString().substring(1);
                DataInputStream in = new DataInputStream(server.getInputStream());
                DataOutputStream out = new DataOutputStream(server.getOutputStream());

                String msg = in.readUTF();
                System.out.println("message received: " + msg + " from " + clientIP);
                
                int clientID = Integer.parseInt(msg.split(",")[1]);
                String fileName=msg.split(",")[5];
               
                // To handle HEARTBEAT messages form servers
                if(msg.contains("HEARTBEAT")){
                	ServerStatus.put(1, false);
                }
                
                // To handle READ request from Clients
                else if (msg.contains("READ")) {
                	Callable<String> metaServerProcess = new MetaServerProcess(msg,prop);
                    Future<String> futureData = pool.submit(metaServerProcess);
                    String content = futureData.get();
                    out.writeUTF(content);
                }
                // To handle WRITE request from Clients
                else if (msg.contains("WRITE")) {
                    Callable<String> metaServerProcess = new MetaServerProcess(msg,prop);
                    Future<String> futureData = pool.submit(metaServerProcess);
                    String content = futureData.get();
                    out.writeUTF(content);
                }
                else if (msg.contains("CREATE")) {
                	Callable<String> metaServerProcess = new MetaServerProcess(msg,prop);
                    Future<String> futureData = pool.submit(metaServerProcess);
                    String content = futureData.get();
                    out.writeUTF(content);
                }
                else if(msg.contains("HEARTBEAT")) {
                	Callable<String> metaServerProcess = new MetaServerProcess(msg,prop);
                    Future<String> futureData = pool.submit(metaServerProcess);
                    String content = futureData.get();
                    out.writeUTF(content);
                }
                else    {
                    out.writeUTF("Operation not supported");
                }

            } catch (SocketTimeoutException s) {
                System.out.println("Socket timed out!");
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            } catch (Exception ex)  {
                ex.printStackTrace();
                break;
            }
        }
        pool.shutdown();
    }
    
    public void cleanDirectory(String path){
    	File file = new File(path);
    	String[] myFiles;    
	    if(file.isDirectory()){
	        myFiles = file.list();
	        for (int i=0; i<myFiles.length; i++) {
	            File myFile = new File(file, myFiles[i]); 
	            myFile.delete();
	        }
	     }
    }
    

    
    
}
