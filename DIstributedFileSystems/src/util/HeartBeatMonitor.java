package util;

import java.util.HashMap;

public class HeartBeatMonitor implements Runnable {

private int DEFAULT_SAMPLING_PERIOD = 5; //seconds
private String DEFAULT_NAME = "HeartbeatMonitor";
private HashMap<Integer, Object> hbstatus; // <id, value>


public HeartBeatMonitor () {
  hbstatus = new HashMap<Integer,Object>();

}


private void collect() {
    /** Here you should collect the data you want to send 
        and store it in the hash
    **/

}

public void sendData(){
    /** Here you should send the data to the server. Use REST/SOAP/multicast messages, whatever you want/need/are forced to **/
}

public void run() {
  System.out.println("Running " +  DEFAULT_NAME );
  try {
     while(true){
        System.out.println("Thread: " + DEFAULT_NAME + ", " + "I'm alive");

        this.collect();
        this.sendData();
        // Let the thread sleep for a while.
        Thread.sleep(DEFAULT_SAMPLING_PERIOD * 1000);
     }
 } catch (InterruptedException e) {
     System.out.println("Thread " +  DEFAULT_NAME + " interrupted.");
 }
 System.out.println("Thread " +  DEFAULT_NAME + " exiting.");
}
}
