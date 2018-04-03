package org.apache.camel.azure.servicebus;

import java.net.URI;

import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;

public class ServiceBusEndpoint extends DefaultEndpoint {

	private String namespace;
	private String sasPolicyKey;
	private String sasPolicyName;
	private String subscriptionName;
	public ServiceBusEndpoint(String endpointUri, Component component) {
		super(endpointUri, component);
	}

	@Override
	public Producer createProducer() throws Exception {
		return new ServiceBusProducer(this);
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		return new ServiceBusConsumer(this, processor);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getSubscriptionName() {
		return subscriptionName;
	}

	public void setSubscriptionName(String subscriptionName) {
		this.subscriptionName = subscriptionName;
	}

	public String getSasPolicyName() {
		return sasPolicyName;
	}

	public void setSasPolicyName(String sasPolicyName) {
		this.sasPolicyName = sasPolicyName;
	}

	public String getSasPolicyKey() {
		return sasPolicyKey;
	}

	public void setSasPolicyKey(String sasPolicyKey) {
		this.sasPolicyKey = sasPolicyKey;
	}

	public boolean isQueue() {
		URI uri = URI.create(getEndpointUri());
		String authority = uri.getAuthority();
		return authority.startsWith("queue:");
	}

	public boolean isTopic() {
		URI uri = URI.create(getEndpointUri());
		String authority = uri.getAuthority();
		return authority.startsWith("topic:");
	}

	public String getDestinationName() {
		URI uri = URI.create(getEndpointUri());
		return uri.getAuthority().split(":")[1];
	}
}
