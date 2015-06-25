package com.mcd.gdw.oozieclient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;

public class DaasOozieClient {
	
	private static List<WorkflowJob> wfJobs;
	private static OozieClient ozclient;
	private static String workflowName;
	
	public static void main(String[] args){
		
		FileInputStream fs = null;
		try{
		ozclient = new OozieClient(args[0]);
		Properties props = new Properties();
		fs = new FileInputStream(args[1]);//"job.properties");
		props.load(fs);
		fs.close();		
	    workflowName=args[2]; // WorkFlow name
	    getListOfRunningJobs(workflowName,ozclient);

        System.out.println("Workflows -->"+wfJobs);
        String jobId="";
        String status="";
        String workflow="";
        System.out.println("Empty--->"+wfJobs.isEmpty());
        while(!wfJobs.isEmpty())
        {
        	Thread.sleep(10*1000);
        	getListOfRunningJobs(workflowName,ozclient);
        	
        }
        if(wfJobs.isEmpty())
        {
        	ozclient.run(props);
        }
        for (WorkflowJob job:wfJobs)
        {
         status=job.getStatus().name();
         workflow= job.getAppName();
         jobId=job.getId();
        	System.out.println("Workflow "+workflow + " with JobId: "+jobId+" is " +status);
        }		 
        /*jobId = ozclient.run(props);
        while(ozclient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING){
			System.out.println(" job running " + jobId + " getid " + jobId);
			try{
				Thread.sleep(10*1000);
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}*/
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				if(fs != null)
					fs.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
	}
	public static void getListOfRunningJobs(String entityName,OozieClient ozclient) throws Exception {
		  StringBuilder builder=new StringBuilder();
		  builder.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.RUNNING).append(';');
		  builder.append(OozieClient.FILTER_NAME).append('=').append(entityName);
		  wfJobs= ozclient.getJobsInfo(builder.toString());
		}
	
}
