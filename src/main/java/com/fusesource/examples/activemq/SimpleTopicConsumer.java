/*
 * Copyright (C) Red Hat, Inc.
 * http://www.redhat.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fusesource.examples.activemq;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTopicConsumer {
	private static final Logger LOG = LoggerFactory
			.getLogger(SimpleProducer.class);

	private static final Boolean NON_TRANSACTED = false;
	private static final String CONNECTION_FACTORY_NAME = "myJmsFactory";
	private static final String DESTINATION_NAME = "simple";
	private static final int MESSAGE_TIMEOUT_MILLISECONDS = 60000;
	private static final int NUM_MESSAGES_TO_BE_RECEIVED = 100;
	private static final String CLIENT_ID = "1";
	private static final String SUBSCRIPTION_NAME = "topicTest";

	public static void main(String args[]) {
		TopicConnection connection = null;

		try {
			// JNDI lookup of JMS Connection Factory and JMS Destination
			Context context = new InitialContext();			
			TopicConnectionFactory factory = (TopicConnectionFactory) context
					.lookup(CONNECTION_FACTORY_NAME);
			Topic topic = (Topic) context.lookup(DESTINATION_NAME);

			connection = factory.createTopicConnection();
			connection.setClientID(CLIENT_ID);
			connection.start();

			TopicSession session = connection.createTopicSession(
					NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
			TopicSubscriber consumer = session.createDurableSubscriber(topic,
					SUBSCRIPTION_NAME);

			LOG.info("Start consuming messages from {} with {} ms timeout",
					topic.toString(), MESSAGE_TIMEOUT_MILLISECONDS);

			double start = 0;
			double stop = 0;
			// Synchronous message consumer
			int i;
			int num = 0;
			for (i = 0; i < NUM_MESSAGES_TO_BE_RECEIVED; i++) {
				Message message = consumer
						.receive(MESSAGE_TIMEOUT_MILLISECONDS);
				if (message != null) {
					if (message instanceof TextMessage) {
						final String text = ((TextMessage) message).getText();
						if (i == 0) {
							start = System.currentTimeMillis();
						}
						num++;
						stop = System.currentTimeMillis();
						LOG.info("Got {}. message: {}", (i++), text);
					}
				} else {
					LOG.info("#########");
					break;
				}
			}

			// final double stop = System.currentTimeMillis();
			// LOG.info("{}Received {} messages in {} seconds - throughput ",
			// new Object[] { stop, i, ((stop - start) / 1000) });
			LOG.info("Received {} messages in {} seconds - throughput {}",
					new Object[] { num, ((stop - start) / 1000),
							(num / ((stop - start) / 1000)) });

			consumer.close();
			session.close();
		} catch (Throwable t) {
			LOG.error("Error receiving message", t);
		} finally {
			// Cleanup code
			// In general, you should always close producers, consumers,
			// sessions, and connections in reverse order of creation.
			// For this simple example, a JMS connection.close will
			// clean up all other resources.
			if (connection != null) {
				try {
					connection.close();
				} catch (JMSException e) {
					LOG.error("Error closing connection", e);
				}
			}
		}
	}
}