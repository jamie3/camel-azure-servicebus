package org.apache.camel.azure.servicebus;

import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultExchange;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.ITopicClient;

@RunWith(MockitoJUnitRunner.class)
public class ServiceBusProducerTest {

	@InjectMocks
	ServiceBusProducer testClass;

	@Mock
	CamelContext camelContext;

	@Mock
	ServiceBusEndpoint endpoint;

	@Mock
	ServiceBusComponent component;

	@Mock
	ITopicClient topicClient;

	@Before
	public void setup() {
		testClass.setTopicClient(topicClient);
	}

	@Test(expected = NullPointerException.class)
	public void test_process_nullBody_shouldThrowException() throws Exception {

		Exchange exchange = new DefaultExchange(camelContext);

		testClass.process(exchange);
	}

	@Test
	public void test_process_stringBody_shouldPass() throws Exception {

		Exchange exchange = new DefaultExchange(camelContext);
		exchange.getIn().setBody("Test");

		testClass.process(exchange);

		ArgumentCaptor<IMessage> captor = ArgumentCaptor.forClass(IMessage.class);

		verify(topicClient, times(1)).send(captor.capture());

		IMessage message = captor.getValue();
		assert new String(message.getBody()).equals("Test");
		assert message.getContentType() == "text/plain";
	}

	@Test
	public void test_process_stringMap_shouldPass() throws Exception {

		Map<String,Object> map = new HashMap<String,Object>();
		map.put("key1", "A");
		map.put("key2", 5.0);
		map.put("key3", 6);

		Exchange exchange = new DefaultExchange(camelContext);
		exchange.getIn().setBody(map);

		testClass.process(exchange);

		ArgumentCaptor<IMessage> captor = ArgumentCaptor.forClass(IMessage.class);

		verify(topicClient, times(1)).send(captor.capture());

		IMessage message = captor.getValue();
		assert message.getContentType() == "application/json";

		String body = new String(message.getBody());
		Gson gson = new Gson();
		Map<String, Object> json = gson.fromJson(body, LinkedTreeMap.class);

		assert json.get("key1").equals("A");
		assert (double)json.get("key2") == 5.0;
		assert (double)json.get("key3") == 6.0;
	}
}