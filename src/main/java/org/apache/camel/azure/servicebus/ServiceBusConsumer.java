package org.apache.camel.azure.servicebus;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;

import com.microsoft.azure.servicebus.ExceptionPhase;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageHandler;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.SubscriptionClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;

public class ServiceBusConsumer extends DefaultConsumer {

	private SubscriptionClient subscriptionClient;

	private ServiceBusEndpoint endpoint;

	ServiceBusConsumer(ServiceBusEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {

		if (endpoint.isTopic()) {

			final String topicString = endpoint.getDestinationName() + "/subscriptions/" + endpoint.getSubscriptionName();

			ConnectionStringBuilder builder = new ConnectionStringBuilder(
				endpoint.getNamespace(),
				topicString,
				endpoint.getSasPolicyName(),
				endpoint.getSasPolicyKey()
			);

			subscriptionClient = new SubscriptionClient(builder, ReceiveMode.PEEKLOCK);

			subscriptionClient.registerMessageHandler(new IMessageHandler() {

				@Override
				public CompletableFuture<Void> onMessageAsync(IMessage msg) {

					Exchange exchange = ServiceBusConsumer.super.getEndpoint().createExchange(ExchangePattern.InOut);

					exchange.getIn().setMessageId(msg.getMessageId());

					boolean endTransaction = false;

					try {
						String json = new String(msg.getBody(), "UTF-8");
						exchange.getIn().setBody(json);
						exchange.getIn().setHeader("label", msg.getLabel());
						exchange.getIn().setHeader("contentType", msg.getContentType());

						log.info("Received message from topic " + topicString + " label=" + msg.getLabel() + " payload=" + json);

						getProcessor().process(exchange);

						endTransaction = true;

					} catch (Exception e) {
						log.error("Failed processing exchange: " + e.getMessage(), e);
					}

					// TODO make this configurable
					if (endTransaction) {
						return subscriptionClient.completeAsync(msg.getLockToken());
					} else {
						return subscriptionClient.abandonAsync(msg.getLockToken());
					}
				}

				@Override
				public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
					log.error("Notify exception", throwable);
				}

			}, new MessageHandlerOptions(1, false, Duration.ofMinutes(1)));

		} else {
			throw new IllegalArgumentException("Invalid URI. Must be topic, or queue.");
		}

		super.doStart();

	}


	@Override
	public void doStop() {
		if (subscriptionClient != null) {
			subscriptionClient.closeAsync();
		}
	}
}
