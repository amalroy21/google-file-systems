import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Random;



public class Client {
	
	// initialize socket and input output streams
    private Socket socket            = null;
    private DataInputStream  input   = null;
    private DataOutputStream output  = null;
	
	class ClientInfo  {
	    private volatile List<String> serverAddress = new ArrayList<>();
	    private String metaServerAddress;
	    private int clientport, serverport, metaserverport;
	    private Socket[] clients;
	    private volatile int timestamp = 1;
	    private int clientID;
	    private int noOfRequests;
	    private ArrayList<String> typeOfOperations;
	    private volatile Boolean isRunning = true;
	    
	}
	
    final ClientInfo clientInfo = new ClientInfo();
    Random rand = new Random();

    public Client(Properties properties, String clientID) {


        for(int i = 1; i <= Integer.valueOf(properties.getProperty("no_of_servers")); i++)
            this.clientInfo.serverAddress.add(properties.getProperty("server" + i));

        /*for(int i = 1; i <= Integer.valueOf(properties.getProperty("noofclient")); i++)
            this.clientInfo.clientAddress.add(properties.getProperty("client"+i));*/
        
        
        this.clientInfo.metaServerAddress = properties.getProperty("metaserver");
        this.clientInfo.metaserverport = Integer.valueOf(properties.getProperty("metaserverport"));
        this.clientInfo.serverport = Integer.valueOf(properties.getProperty("serverport"));
        this.clientInfo.clientport = Integer.valueOf(properties.getProperty("clientport"));
        this.clientInfo.clients = new Socket[this.clientInfo.serverAddress.size()];
        this.clientInfo.noOfRequests = Integer.valueOf(properties.getProperty("noofrequests"));
        this.clientInfo.typeOfOperations = new ArrayList<>();
        this.clientInfo.typeOfOperations.add("CREATE");
        this.clientInfo.typeOfOperations.add("READ");
        this.clientInfo.typeOfOperations.add("WRITE");
        this.clientInfo.clientID = Integer.parseInt(clientID);
    
    }

    public void startClient()   {
        try {
            TCPClientProcess process = new TCPClientProcess();
            System.out.println("-------- Client Started with ID:" + clientInfo.clientID+" --------");
            new Thread(process).start();
        }   catch(Exception ex) {
            ex.printStackTrace();
        }
    }

        public class TCPClientProcess implements Runnable   {

        @Override
        public void run()   {
            try {
                int serverID = 0, operationID, fileID;
                int i = 0,j=0;
                Socket metasoc,sersoc;
                String msg,operation;
                int serID;
                
                
                while(j < clientInfo.noOfRequests) {
                    j++;
                    Thread.sleep(5000);
                    System.out.println("---------------------New Client Operation------------------------------");
                    System.out.println("Operation No: " + j);
                    serverID = rand.nextInt(clientInfo.serverAddress.size());
                    operationID = rand.nextInt(3);
                    //operationID=0;
                    fileID = rand.nextInt(5) + 1;
                    operation=clientInfo.typeOfOperations.get(operationID);
                    
                    //Query to meta server to get the details 
                    System.out.println("Operation="+operation+"FileID:"+fileID);
                    metasoc = new Socket(clientInfo.metaServerAddress, clientInfo.metaserverport);
                    output = new DataOutputStream(metasoc.getOutputStream());
                    input = new DataInputStream(metasoc.getInputStream());
                    
                    output.writeUTF("Client ID," + clientInfo.clientID + ",Operation," + operation +",File,"+ fileID+".txt");
                    msg=input.readUTF();
                    
                    output.close();
                    input.close();
                    metasoc.close();
                    if(msg.contains("ERROR")||!msg.contains("Server")){
                    	continue;
                    }
                    System.out.println("Chunk Details :"+msg);
                    if(operation.contains("READ")){
                    	
                    	String[] chunkList=msg.split("\\|\\|");
                    	int noofchunks=chunkList.length;
                    	String[][] chunkdetail=new String[noofchunks][3]; 
                    	i=0;
                    	for(String s:chunkList){
                    		System.out.println(s);
                    		chunkdetail[i]=s.split(",");
                    		i++;
                    	}
                    	
                    	for(i=0;i<noofchunks;i++){
                    		System.out.println(noofchunks);
                    		//fileID=Integer.parseInt(fileName.replace(".txt", ""));
                        	String fname = fileID+"_"+chunkdetail[i][2]+".txt";
                    		serID=Integer.parseInt(chunkdetail[i][1].replace("Server",""));
                    		System.out.println(serID);
                    		sersoc = new Socket(clientInfo.serverAddress.get(serID-1).trim(), clientInfo.serverport);
                    		output = new DataOutputStream(sersoc.getOutputStream());
                            input = new DataInputStream(sersoc.getInputStream());

                            output.writeUTF("Client ID," + clientInfo.clientID + ",Operation," + operation +",File,"+ fname);
                            System.out.println(input.readUTF());
                            input.close();
                            output.close();
                            sersoc.close();
                    	}
                    
                    }else if (operation.contains("WRITE")){
                    		
                    		String[] chunkList=msg.split("\\|\\|");
	                    	int noofchunks=chunkList.length;
	                    	String[][] chunkdetail=new String[noofchunks][3]; 
	                    	i=0;
	                    	for(String s:chunkList){
	                    		System.out.println(s);
	                    		chunkdetail[i]=s.split(",");
	                    		i++;
	                    	}
	                    	
	                    	for(i=0;i<noofchunks;i++){
	                    		System.out.println(noofchunks);
	                    		//fileID=Integer.parseInt(fileName.replace(".txt", ""));
	                        	String fname = fileID+"_"+chunkdetail[i][2]+".txt";
	                    		serID=Integer.parseInt(chunkdetail[i][1].replace("Server",""));
	                    		System.out.println(serID);
	                    		sersoc = new Socket(clientInfo.serverAddress.get(serID-1).trim(), clientInfo.serverport);
	                    		output = new DataOutputStream(sersoc.getOutputStream());
	                            input = new DataInputStream(sersoc.getInputStream());
	
	                            output.writeUTF("Client ID," + clientInfo.clientID + ",Operation," + operation +",File,"+ fname);
	                            System.out.println(input.readUTF());
	                            input.close();
	                            output.close();
	                            sersoc.close();
                    }
                    
                    
                    }
                    metasoc.close();
                    input.close();
                    System.out.println("Operation Successful!");
                   }

                    clientInfo.timestamp++;
                    System.out.println("Request Over");
                    System.out.println("-------------------------------------");
                    Thread.sleep(1000);
                
        
                System.out.println();
                System.out.println("Processing Done");
                Thread.sleep(10000);
            }
            catch(ConnectException conEx)   {
                System.out.println("Server not up yet. Please start servers before starting client" + conEx.getMessage());
            }
            catch (IOException ioEx) {
                ioEx.printStackTrace();
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
            finally {
                clientInfo.isRunning = false;
            }
        }
    }

}

