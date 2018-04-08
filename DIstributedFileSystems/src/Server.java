import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import util.*;

public class Server {

    private ServerSocket soc;
    private String directoryPath;
    private String clean;
    private final String ServerID;
    private Properties prop;

    public Server(Properties properties, String serverID) throws IOException {
        this.soc = new ServerSocket(Integer.valueOf(properties.getProperty("serverport")));
        this.directoryPath = properties.getProperty("serverDir"+serverID);
        this.ServerID = serverID;
        this.clean = properties.getProperty("clean");
        this.prop = properties;
    }

    /**
     * Start the server to start listening from clients
     */
    @SuppressWarnings("unchecked")
	public void startServer()   {

        System.out.println("------------------Server" + ServerID + " Started ---------------------------");
        System.out.println("Waiting for client on port : " +soc.getLocalPort() + " ...");
        System.out.println();
        ExecutorService pool = Executors.newFixedThreadPool(3);
        
        if("true".equalsIgnoreCase(clean)){
        	cleanDirectory(directoryPath);
        }
        
        HEARTBEATProcess process = new HEARTBEATProcess();
        //Thread.sleep(5000);
        System.out.println("-------- HEARTBEAT STARTED:" + ServerID+" ---------------------");
        new Thread(process).start();
        
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
                
                if (msg.contains("CREATE")) {
                	Callable<Boolean> createFileCallable = new CreateFile(directoryPath, fileName);
                    Future<Boolean> futureCreate = pool.submit(createFileCallable);
                    Boolean flag = futureCreate.get();
                   
                    if (flag) {
                        out.writeUTF("File Successfully Created");
                    } else {
                        out.writeUTF("File Not Created");
                    }
                }
                
                // To handle READ request from Clients
                else if (msg.contains("READ")) {
                    Callable<String> readFileCallable = new ReadFile(directoryPath, fileName);
                    Future<String> futureRead = pool.submit(readFileCallable);
                    String content = futureRead.get();
                    //System.out.println(content);
                    out.writeUTF(content);
                }
                // To handle WRITE request from Clients
                else if (msg.contains("WRITE")) {
                    Callable<Boolean> writeFileCallable = new WriteFile(directoryPath, fileName,msg);
                    Future<Boolean> futureWrite = pool.submit(writeFileCallable);
                    Boolean flag = futureWrite.get();
                    if (flag) {
                        out.writeUTF("File Successfully Updated");
                    } else {
                        out.writeUTF("File Not Updated");
                    }
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
    
    @SuppressWarnings("unchecked")
	public class HEARTBEATProcess implements Runnable{
    	@Override
        public void run(){
	    	try {
	    		while(true){
	    			Thread.sleep(2000);
			    	String MetaDataServer = prop.getProperty("metaserver");
			    	int port = Integer.valueOf(prop.getProperty("metaport"));
			        Socket metadataServer = new Socket(MetaDataServer, port);
			        DataInputStream in = new DataInputStream(metadataServer.getInputStream());
			        DataOutputStream out = new DataOutputStream(metadataServer.getOutputStream());
			        ExecutorService pool = Executors.newFixedThreadPool(2);
			        Callable<String> dirFilesCallable = new DirFiles(directoryPath);
			        Future<String> futureDirFiles = pool.submit(dirFilesCallable);
			        String content = futureDirFiles.get();
			        String msg="HEARTBEAT:" + ServerID + ";" + content;
			        out.writeUTF(msg);
			        //String msg = in.readUTF();
			        System.out.println(msg);
			        pool.shutdown();
			        in.close();
			        out.close();
			        metadataServer.close();
			    } 
	    	}catch (Exception ex) {
			        System.out.println("");
			    
	    	}
	    	
    	}
    }
}