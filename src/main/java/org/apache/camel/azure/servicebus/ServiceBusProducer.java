package org.apache.camel.azure.servicebus;

import java.time.Instant;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;

import com.google.gson.Gson;
import com.microsoft.azure.servicebus.ITopicClient;
import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.TopicClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;

public class ServiceBusProducer extends DefaultProducer {

	private ITopicClient topicClient;

	public ServiceBusProducer(ServiceBusEndpoint endpoint) {
		super(endpoint);
	}

	public void setTopicClient(ITopicClient topicClient) {
		this.topicClient = topicClient;
	}

	public ITopicClient getTopicClient() {
		return this.topicClient;
	}

	@Override
	protected void doStart() throws Exception {

		ServiceBusEndpoint sbEndpoint = (ServiceBusEndpoint) getEndpoint();

		if (sbEndpoint.isTopic()) {
			ConnectionStringBuilder builder = new ConnectionStringBuilder(sbEndpoint.getNamespace(),
					sbEndpoint.getDestinationName(), sbEndpoint.getSasPolicyName(), sbEndpoint.getSasPolicyKey());

			topicClient = new TopicClient(builder);

		} else {
			throw new IllegalArgumentException("Invalid URI. Expected topic or queue.");
		}

		super.doStart();
	}

	@Override
	public void process(Exchange exchange) throws Exception {

		Object body = exchange.getIn().getBody();
		if (body == null) {
			throw new NullPointerException("Body is null");
		}

		String label = exchange.getIn().getHeader("label", String.class);
		String contentType = exchange.getIn().getHeader("contentType", String.class);
		Instant scheduledEnqueuedTimeUtc = exchange.getIn().getHeader("scheduledEnqueuedTimeUtc", Instant.class);

		if (body instanceof Map) {
			Gson gson = new Gson();
			body = gson.toJson(body);
			contentType = "application/json";
		} else if (body instanceof String) {
			contentType = "text/plain";
		}

		Message message = new Message((String) body);
		if (contentType != null) {
			message.setContentType(contentType);
		}
		if (label != null) {
			message.setLabel(label);
		}

		if (scheduledEnqueuedTimeUtc != null) {
			message.setScheduledEnqueuedTimeUtc(scheduledEnqueuedTimeUtc);
		}

		topicClient.send(message);
	}
}
