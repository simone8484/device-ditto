/*
 * Copyright (c) 2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package com.tesi.app.device;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scripts {
	
	public ProcessBuilder pb;
	public Process p;
	private String pid;
	private boolean skip;
	public String mqttUrl = "NAN";
	public int frequency = -1;
	private static final int BUFFER_SIZE = 4096;
    ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(Scripts.class);
	public Scripts() {
		this.pb = new ProcessBuilder("node","C:\\Users\\simone\\Desktop\\Mosca_MQTTjs_MySQL_MongoDB-master\\pub.js");
		this.pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		this.pb.redirectError(ProcessBuilder.Redirect.INHERIT);
		this.skip =  false;
	}
	
	public boolean exec(String url,  int frequency){
		 Runtime rt = Runtime.getRuntime();
         try {
			this.p = Runtime.getRuntime().exec("cmd /c cd src\\main\\resources\\firmware && nar run digital_twin-1.0.0.nar --args-start '"+url+","+frequency+"'");
			
		//	System.out.println(this.p.pid());
	         BufferedReader stdInput = new BufferedReader(new 
	              InputStreamReader(this.p.getInputStream()));

	         BufferedReader stdError = new BufferedReader(new 
	              InputStreamReader(this.p.getErrorStream()));

	         // Read the output from the command
	         System.out.println("Here is the standard output of the command:\n");
	         String s = null;
	         while ((s = stdInput.readLine()) != null) {
	        	 if(s.contains("PROCESS_PID::")) {
	        		// config.setProperty("ditto.mqtt.pid", s.split("::")[1]);
	        		 setPid(s.split("::")[1]);
	        	 }
	             System.out.println(s);
	         }

	         // Read any errors from the attempted command
	         System.out.println("Here is the standard error of the command (if any):\n");
	         while ((s = stdError.readLine()) != null) {
	             System.out.println(s);
	         }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
        return true;
	}
	
	public boolean stop() {
		String tokill = null;
		//tokill = config.getProperty("ditto.mqtt.pid");
		tokill = getPid();
		String cmd = "taskkill /F /PID " + tokill;
		try {
			Runtime.getRuntime().exec(cmd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		setPid("-1");
		return true;
	}

	
	public Boolean estractFileTar(String baseDir) throws IOException {
	        String so = System.getProperty("os.name");
	        if(so.contains("Windows")) {
		        Process process = Runtime.getRuntime().exec("tar -xf FirmwareNodeJs.zip -C C:\\Users\\simon\\eclipse-workspace\\Device\\device\\src\\main\\resources\\firmaware");
		      ///  process.isAlive()
	        }else {
	        	//Process process = Runtime.getRuntime().exec("unzip FirmwareNodeJs.zip -C C:\\Users\\simon\\eclipse-workspace\\Device\\device\\src\\main\\resources\\firmaware");
	        	Process process = Runtime.getRuntime().exec("unzip FirmwareNodeJs.zip -C "+baseDir+"Device\\device\\src\\main\\resources\\firmaware");
	        }         
	        
	          Process process1 = Runtime.getRuntime().exec("cmd /c cd firmaware && nar create");	         
	          return true;
	}
	
   public String getPid() {
	   return this.pid;
   }
   
   public void setPid(String pid) {
	   this.pid = pid;
   }

	public boolean isSkip() {
		return skip;
	}

	public void setSkip(boolean skip) {
		this.skip = skip;
	}
   
   

}
