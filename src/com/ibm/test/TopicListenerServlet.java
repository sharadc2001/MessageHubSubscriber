package com.ibm.test;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.messagehub.samples.MessageHubJavaSample;

public class TopicListenerServlet implements ServletContextListener {

	private MessageHubJavaSample ms;
	private String topic = "usa";
	private String kafkaHost="kafka01-prod01.messagehub.services.us-south.bluemix.net:9093,kafka02-prod01.messagehub.services.us-south.bluemix.net:9093,kafka03-prod01.messagehub.services.us-south.bluemix.net:9093,kafka04-prod01.messagehub.services.us-south.bluemix.net:9093,kafka05-prod01.messagehub.services.us-south.bluemix.net:9093";
	private String apiKey="4vyQ0exjKidRtclzehEWN0UFoNWuXDCsjkVKBmGqYQn1MUKK";
	
	
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {

		
		// Your code here
		System.out.println("HelloWorld Listener has been shutdown");

	}

	@Override
	public void contextInitialized(ServletContextEvent servletContextEvent) {

		// Your code here
		ms=new MessageHubJavaSample(topic);
		System.out.println("HelloWorld Listener initialized.");

		TimerTask vodTimer = new VodTimerTask();

		Timer timer = new Timer();
		timer.schedule(vodTimer, 1000, (2 * 1000));

	}

	class VodTimerTask extends TimerTask {

		@Override
		public void run() {
			System.out.println("TimerTask " + new Date().toString());
            try{ ms.ConsumeData();}catch(Exception t){t.printStackTrace();}
		}
	}

}