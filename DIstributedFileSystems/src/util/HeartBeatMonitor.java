package util;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HeartBeatMonitor implements Runnable {

	private int RunPeriod = 1; //seconds
	private String DEFAULT_NAME = "HeartbeatMonitor";
	static HashMap<Integer, Object> hbstatus; // <id, value>
	private int no_of_servers;
	private int checkPeriod;
	
	public class ServerInfo{
		private int id;
		private String serverAddress ;
	    private volatile int timestamp;
	    private volatile Instant lastUpdatedTimeStamp;
	    private volatile Boolean isRunning;
	    private Map<String, Map> fileDetail;
	
	    public ServerInfo(int id, String address) {
	        this.id = id;
	        this.isRunning = false;
	        this.timestamp=0;
	        this.serverAddress = address;
	        this.lastUpdatedTimeStamp = Instant.now();
	        this.fileDetail = new LinkedHashMap<>();
	    }
	    
	    public void serverHB(String file){
	    	this.isRunning = true;
	    	this.lastUpdatedTimeStamp = Instant.now();
	    }
	}
	
	public HeartBeatMonitor (Properties prop) {
	  hbstatus = new HashMap<Integer,Object>();
	  no_of_servers = Integer.valueOf(prop.getProperty("no_of_servers"));
      for(int i=1;i<=no_of_servers;i++){
      	ServerInfo SI = new ServerInfo(i, prop.getProperty("server"+i));
      	hbstatus.put(i, SI);
      }
      this.checkPeriod = Integer.valueOf(prop.getProperty("HBcheckPeriod"));
	}

	
	public void run() {
	  System.out.println("Running " +  DEFAULT_NAME );
	  try {
	     while(true){
	    	for(int i=1;i<=no_of_servers;i++){
	    		
	    		ServerInfo SI=(ServerInfo) hbstatus.get(i);
	    		Duration duration = Duration.between(SI.lastUpdatedTimeStamp,Instant.now());
	    		//System.out.println(duration.toMillis());
	    		if(duration.toMillis() >= (checkPeriod * 1000)){
	    			System.out.println("Server Down"+SI.id);
	    			SI.isRunning=false;
	    		}
	    	}
	        Thread.sleep(RunPeriod * 1000);
	     }
	 } catch (InterruptedException e) {
	     System.out.println("Thread " +  DEFAULT_NAME + " interrupted.");
	 }
	 System.out.println("Thread " +  DEFAULT_NAME + " exiting.");
	}
}
