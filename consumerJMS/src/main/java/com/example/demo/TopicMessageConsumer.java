package com.example.demo;

import java.io.IOException;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.transport.TransportListener;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * A simple message consumer which consumes the message from ActiveMQ Broker 
 * 
 *
 */
@Service 
public class TopicMessageConsumer implements MessageListener,ExceptionListener, TransportListener, InitializingBean, DisposableBean {

	@Value("${activemq.broker-url}")
	private String activeMqBrokerUri;
	
	@Value("${activemq.user}")
	private String username;
	
	@Value("${activemq.password}")
	private String password;
	
	@Value("${activemq.topic}")
	private String topicDestination;
	
	private Connection connection;
	
	private Session session;

	private long ms=0;
	
	public void run() throws JMSException {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(username, password, activeMqBrokerUri);
		factory.setTransportListener(this);
		connection = factory.createConnection();
		connection.setClientID("clientID");
		connection.setExceptionListener(this);
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Topic destination = session.createTopic(topicDestination);
		MessageConsumer consumer = session.createDurableSubscriber(destination,"subsname");
		consumer.setMessageListener(this);
		System.out.println(String.format("TopicMessageConsumer Waiting for messages at %s %s", topicDestination, this.activeMqBrokerUri));
	}

	@Override
	public void onMessage(Message message) {
		String msg;
		try {
			msg = ((TextMessage) message).getText().replaceAll("#", "");
			System.out.println(new Date()+ " ["+ms+"] "+ msg);
			ms++;
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void destroy() throws Exception {
		session.close();
		connection.close();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		run();
	}

	@Override
	public void onException(JMSException exception) {
		System.out.println("Excepcion---------------------");
		
	}

	@Override
	public void onCommand(Object command) {
		System.out.println("onCommand TL ---------------------");// TODO Auto-generated method stub
		
	}

	@Override
	public void onException(IOException error) {
		System.out.println("Excepción del TL ---------------------");
		
	}

	@Override
	public void transportInterupted() {
		System.out.println("TransportInterupted TL ---------------------");// TODO Auto-generated method stub
		
	}

	@Override
	public void transportResumed() {
		System.out.println("TransportResumed TL ---------------------");// TODO Auto-generated method stub
		
	}

}
