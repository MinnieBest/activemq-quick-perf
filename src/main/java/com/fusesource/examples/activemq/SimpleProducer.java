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

import java.text.DecimalFormat;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer {
	private static final Logger LOG = LoggerFactory
			.getLogger(SimpleProducer.class);

	private static final Boolean NON_TRANSACTED = false;
	private static final long MESSAGE_TIME_TO_LIVE_MILLISECONDS = 0;
	private static final int NUM_MESSAGES_TO_BE_SENT = 100;
	private static final String CONNECTION_FACTORY_NAME = "myJmsFactory";
	private static final String DESTINATION_NAME = "simple";

	public static void main(String args[]) {
		Connection connection = null;

		try {
			// JNDI lookup of JMS Connection Factory and JMS Destination
			Context context = new InitialContext();
			ConnectionFactory factory = (ConnectionFactory) context
					.lookup(CONNECTION_FACTORY_NAME);
			Destination destination = (Destination) context
					.lookup(DESTINATION_NAME);
			

			connection = factory.createConnection();
			connection.start();

			Session session = connection.createSession(NON_TRANSACTED,
					Session.AUTO_ACKNOWLEDGE);
			
			MessageProducer producer = session.createProducer(destination);

			producer.setTimeToLive(MESSAGE_TIME_TO_LIVE_MILLISECONDS);
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);

			LOG.info("Starting to send {} messages to {}...",
					NUM_MESSAGES_TO_BE_SENT, destination.toString());

			final double start = System.currentTimeMillis();
			for (int i = 1; i <= NUM_MESSAGES_TO_BE_SENT; i++) {
				final TextMessage message = session.createTextMessage(i
						+ ". message sent");
				// LOG.debug("Sending to destination: {} this text: {}",
				// destination.toString(), message.getText());
				// final double item1 = System.currentTimeMillis();
				producer.send(message);
				// final double item2 = System.currentTimeMillis();
				// LOG.info("Sent item {} in {} seconds ", new Object[] { i,
				// ((item2 - item1) / 1000)});
			}

			final double stop = System.currentTimeMillis();

			LOG.info("Sent {} messages in {} seconds - throughput {}",
					new Object[] { NUM_MESSAGES_TO_BE_SENT,
							((stop - start) / 1000),
							(NUM_MESSAGES_TO_BE_SENT * 1000 / (stop - start)) });

			// Cleanup
			producer.close();
			session.close();
		} catch (Throwable t) {
			LOG.error("Error sending message", t);
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
