
package util;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import util.HeartBeatMonitor.ServerInfo;

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
    private int chunksize;
    
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
        if(!Data.contains("HEARTBEAT")){
	        this.clientID = Integer.parseInt(Data.split(",")[1]);
	        this.fileName = Data.split(",")[5];
	        this.operation = Data.split(",")[3];
        }else{
        	this.operation = "HEARTBEAT";
        }
        this.chunksize = Integer.valueOf(properties.getProperty("chunksize"));
    }

    public String call()   {
        try {
        	
        	String msg="";
        	
        	//To Handle Heart Beat message from servers
        	if(operation.contains("HEARTBEAT")) {
            	heartBeat(data);
            }
        	else if(operation.contains("CREATE")){
        		msg=createNewFile(1);
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
	            	Socket server = new Socket(serverList.get(serID-1),serverPort );
	                DataInputStream in = new DataInputStream(server.getInputStream());
	                DataOutputStream out = new DataOutputStream(server.getOutputStream());
	                String fileID=fileName.replace(".txt", "");
	                String fname = fileID+"_"+chunkdetail[2]+".txt";
	                msg = "Client ID,"+clientID+",Operation,READ,File,"+ fname;
	                out.writeUTF(msg);
	                content = in.readUTF();
	                System.out.println(content);
	                server.close();
	                if((content.length()+msg.length())>chunksize){
	                	int n = Integer.parseInt(chunkdetail[2]);
	                	msg = createNewFile(n+1);
	                	fileReader = new FileReader(dir + "/" + fileName);
	                	bufferedReader = new BufferedReader(fileReader);
			
			            while ((line = bufferedReader.readLine())!= null) {
			                content += line;
			            }
			            bufferedReader.close();
			            fileReader.close();
			            System.out.println("New File Contents:"+content);
			            chunkList=content.split("\\|\\|");
		            	noofchunks=chunkList.length;
		            	
		            	chunkdetail=chunkList[noofchunks-1].split(","); 
		            	i=0;
		            	serID=Integer.parseInt(chunkdetail[1].replace("Server", ""));
		            	/*server = new Socket(serverList.get(serID-1),serverPort );
		                in = new DataInputStream(server.getInputStream());
		                out = new DataOutputStream(server.getOutputStream());
		                fileID=fileName.replace(".txt", "");
		                fname = fileID+"_"+chunkdetail[2]+".txt";
		                msg = "Client ID,"+clientID+",Operation,READ,File,"+ fname;
		                out.writeUTF(msg);
		                content = in.readUTF();
		                System.out.println(content);*/
		                //server.close();
	                }
	                
	                msg=fileName+",Server"+serID+","+chunkdetail[2]+"||";
	                
                }else{
                	msg=createNewFile(1);
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
    
    public String createNewFile(int chunk){
    	String msg="";
    	Random rand = new Random();
    	String fname=fileName.replace(".txt", "")+"_"+chunk+".txt";
		int serID = rand.nextInt(serverList.size())+1;
		Socket server;
		try {
			File file = new File(dir + "/" + fileName);
			if(!file.exists()){
				server = new Socket(serverList.get(serID-1),serverPort );
		        DataOutputStream out = new DataOutputStream(server.getOutputStream());
		        DataInputStream in = new DataInputStream(server.getInputStream());
		        out.writeUTF("Client ID,"+clientID+",Operation,CREATE,File,"+ fname);
		        msg = in.readUTF();
		        server.close();
		        if(msg.contains("File is created!")){
		      	  	//Create new chunk
		        	file.createNewFile();
	        		System.out.println("Chunk created!");
	        		FileReader fileReader = new FileReader(dir + "/" + fileName);
		            BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		            String line = "", content = "";
		            while ((line = bufferedReader.readLine())!= null) {
		                content += line;
		            }
		            if(content.length()>1){
		            	content+="||";
		            }
		            bufferedReader.close();
		            fileReader.close();
		
	        		FileWriter fw = new FileWriter(dir + "/" + fileName);
	                BufferedWriter bw = new BufferedWriter(fw);
	                msg=content+fileName+",Server"+serID+","+chunk+"||";
	                System.out.println("Chunk Entry -"+msg);
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
    	int sid = Integer.parseInt(serverID.replace("Server",""));
    	
    	if(msg.contains(",")){
    		msg=msg.split(";")[1];
    		String filelist[]=msg.split(",");
    	}
    	String data = "";
    	ServerInfo SI = (ServerInfo) HeartBeatMonitor.hbstatus.get(sid);
    	SI.serverHB(msg);
    	HeartBeatMonitor.hbstatus.put(sid, SI);
    	
    }
}
