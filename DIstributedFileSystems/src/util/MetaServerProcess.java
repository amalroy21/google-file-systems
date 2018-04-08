
package util;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

@SuppressWarnings("rawtypes")
public class MetaServerProcess implements Callable {

    private String dir;
    private String fileName;
    private String operation;
    private String data;
    private List<String> serverList = new ArrayList<>();
    private int serverPort;
    private Properties prop;
    private int clientID;
    private List<Boolean> serverStatus = new ArrayList<>();
    
    public MetaServerProcess(String Data,Properties properties)  {
        
        /*this.ServerList = ServerList;
        this.serverPort = ServerPort;*/
        this.data = Data;
        this.prop = properties;
        int n = Integer.valueOf(properties.getProperty("no_of_servers"));
        for(int i=1;i<=n;i++){
        	this.serverList.add(properties.getProperty("server"+i));
        }
        this.serverPort = Integer.valueOf(properties.getProperty("serverport"));
        this.dir = properties.getProperty("metaDir");
        this.clientID = Integer.parseInt(Data.split(",")[1]);
        this.fileName = Data.split(",")[5];
        this.operation = Data.split(",")[3];
    }

    public String call()   {
        try {
        	String msg="";
        	
        	System.out.println("Inside Meta Server Processor");
        	if(operation.contains("CREATE")){
        		msg=createNewFile();
        	}
            // To handle WRITE request from Clients
            else if (operation.contains("WRITE")) {
            	
            	String fileDir=dir + "/" + fileName;
            	System.out.println("Inside Write : "+fileDir);
            	
            	File file = new File(fileDir);
                if(file.exists())
                {
	            	FileReader fileReader = new FileReader(dir + "/" + fileName);
		            BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		            String line = "", content = "";
		            while ((line = bufferedReader.readLine())!= null) {
		                content = line;
		            }
		            bufferedReader.close();
		            fileReader.close();
		            System.out.println("File Contents:"+content);
		            String chunkList[]=content.split("\\|\\|");
	            	int noofchunks=chunkList.length;
	            	
	            	String[] chunkdetail=chunkList[noofchunks-1].split(","); 
	            	int i=0;
	            	int serID=Integer.parseInt(chunkdetail[1].replace("Server", ""));
	            	/*Socket server = new Socket(serverList.get(serID-1),serverPort );
	                //String clientIP = server.getInetAddress().toString().substring(1);
	                DataInputStream in = new DataInputStream(server.getInputStream());
	                DataOutputStream out = new DataOutputStream(server.getOutputStream());
	                //String fileID=fileName.replace(".txt", "");
	                //String fname = fileID+"_"+chunkdetail[2]+".txt";
	                out.writeUTF("Client ID,"+clientID+",Operation,WRITE,File,"+ fileName+","+chunkdetail[2]);
	                server.close();*/
	                msg=fileName+",Server"+serID+","+chunkdetail[2]+"||";
	                
                }else{
                	msg=createNewFile();
                }
	            
            }else if(operation.contains("READ")){
            	String fileDir=dir + "/" + fileName;
            	System.out.println("Inside READ : "+fileDir);
            	
            	File file = new File(fileDir);
                if(file.exists())
	                {
	            	FileReader fileReader = new FileReader(fileDir);
		            BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		            String line = "";
		            while ((line = bufferedReader.readLine())!= null) {
		                msg = line;
		            }
		            bufferedReader.close();
		            fileReader.close();
	            }
                
	            else if(msg.contains("HEARTBEAT")) {
	            	heartBeat(msg);
	            }
                
                else{
                	msg="ERROR: No such file exists!";
                	System.out.println(msg);
                }
            }
            
        	return msg;
        }
        catch(IOException ex) {
            return "ERROR : Writing/Reading file '" + fileName + "'";
        }
    }
    
    public String createNewFile(){
    	String msg;
    	Random rand = new Random();
		int serID = rand.nextInt(serverList.size())+1;
		Socket server;
		try {
			server = new Socket(serverList.get(serID-1),serverPort );
		
	        DataOutputStream out = new DataOutputStream(server.getOutputStream());
	        DataInputStream in = new DataInputStream(server.getInputStream());
	        out.writeUTF("Client ID,"+clientID+",Operation,CREATE,File,"+ fileName);
	        msg = in.readUTF();
	        server.close();
	        if("File Not Created".equals(msg)){
	        
	            File file = new File(dir + "/" + fileName);
	      	  	//Create new chunk
	        	if (file.createNewFile()){
	        		System.out.println("File is created!");
	        		FileWriter fw = new FileWriter(dir + "/" + fileName);
	                BufferedWriter bw = new BufferedWriter(fw);
	                msg=fileName+",Server"+serID+",1||";
	                bw.write(msg);
	                bw.close();
	        	}
	        }
	    	else{
	    		System.out.println("File already exists.");
	    	}
        return msg;
		} catch(IOException ex) {
	        System.out.println("Error Creating file '" + fileName + "'");
	        return String.valueOf(ex.getStackTrace());
	    }
    }
    
    public void heartBeat(String msg){
    	
    	msg=msg.replace("HEARTBEAT:", "");
    	String serverID = msg.split(";")[0];
    	msg=msg.split(";")[1];
    	String chunkList[]=msg.split("\\|\\|");
    	int noofchunks=chunkList.length;
    	String[][] chunkdetail=new String[noofchunks][3]; 
    	int i=0;
    	for(String s:chunkList){
    		chunkdetail[i]=s.split(",");
    		i++;
    		
    	}
    	
    	serverStatus.add(true);
    	
    	
    }
}
