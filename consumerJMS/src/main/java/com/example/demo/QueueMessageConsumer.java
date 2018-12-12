package com.example.demo;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * A simple message consumer which consumes the message from ActiveMQ Broker 
 * 
 * @author Mary.Zheng
 *
 */
@Service
public class QueueMessageConsumer implements MessageListener, InitializingBean, DisposableBean {

	@Value("${activemq.broker-url}")
	private String activeMqBrokerUri;
	
	@Value("${activemq.user}")
	private String username;
	
	@Value("${activemq.password}")
	private String password;
	
	@Value("${activemq.queue}")
	private String destinationName;
	
	private Connection connection;
	
	private Session session;
	
	public void run() throws JMSException {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(username, password, activeMqBrokerUri);
		connection = factory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue(destinationName);
		MessageConsumer consumer = session.createConsumer(destination);
		consumer.setMessageListener(this);
		System.out.println(String.format("QueueMessageConsumer Waiting for messages at %s %s", destinationName, this.activeMqBrokerUri));
	}

	@Override
	public void onMessage(Message message) {
		String msg;
		try {
			msg = String.format("QueueMessageConsumer Received message [ %s ]",((TextMessage) message).getText());
			System.out.println(msg);
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

}
