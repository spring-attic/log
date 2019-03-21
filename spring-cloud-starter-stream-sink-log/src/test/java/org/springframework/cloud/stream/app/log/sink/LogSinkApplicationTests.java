/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.log.sink;

import java.util.HashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.converter.TupleJsonMessageConverter;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.tuple.Tuple;
import org.springframework.tuple.TupleBuilder;
import org.springframework.util.MimeType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest({ "server.port=-1", "log.name=foo", "log.level=warn",
		"log.expression=payload.toUpperCase()" })
public class LogSinkApplicationTests {

	@Autowired
	private Sink sink;

	@Autowired
	private LoggingHandler logSinkHandler;

	@Test
	public void testTextContentType() {
		Message<byte[]> message = MessageBuilder.withPayload("{\"foo\":\"bar\"}".getBytes())
				.setHeader("contentType", MimeType.valueOf("text/plain"))
				.build();
		this.testMessage(message, "{\"foo\":\"bar\"}");
	}

	@Test
	public void testJsonContentType() {
		Message<byte[]> message = MessageBuilder.withPayload("{\"foo\":\"bar\"}".getBytes())
				.setHeader("contentType", new MimeType("json"))
				.build();
		this.testMessage(message, "{\"foo\":\"bar\"}");
	}

	@Test
	public void testTupleContentType() {
		Tuple tuple = TupleBuilder.tuple().of("foo", "bar");
		Message<byte[]> message = (Message<byte[]>) new TupleJsonMessageConverter(
				new ObjectMapper()).toMessage(tuple, new MessageHeaders(new HashMap<>()));
		this.testMessage(message, "{\"foo\":\"bar\"}");
	}

	private void testMessage(Message<byte[]> message, String expectedPayload) {
		assertNotNull(this.sink.input());
		assertEquals(LoggingHandler.Level.WARN, this.logSinkHandler.getLevel());
		Log logger = TestUtils.getPropertyValue(this.logSinkHandler, "messageLogger",
				Log.class);
		assertEquals("foo", TestUtils.getPropertyValue(logger, "logger.name"));
		logger = spy(logger);
		new DirectFieldAccessor(this.logSinkHandler).setPropertyValue("messageLogger",
				logger);
		this.sink.input().send(message);
		ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
		verify(logger).warn(captor.capture());
		assertEquals(expectedPayload.toUpperCase(), captor.getValue());
		this.logSinkHandler.setLogExpressionString("#this");
		this.sink.input().send(message);
		verify(logger, times(2)).warn(captor.capture());

		Message captorMessage = (Message) captor.getAllValues().get(2);
		assertEquals("Unexpected payload value", expectedPayload, captorMessage.getPayload());

		MessageHeaders messageHeaders = captorMessage.getHeaders();
		assertEquals("Unexpected number of headers", 3, messageHeaders.size());

		String[] headers = { "contentType" };

		for (String header : headers) {
			assertTrue("Missing " + header + " header", messageHeaders.containsKey(header));
			assertEquals("Header " + header + " does not match", messageHeaders.get(header),
					message.getHeaders().get(header));
		}
	}

	@SpringBootApplication
	static class LogSinkApplication {
		public static void main(String[] args) {
			SpringApplication.run(LogSinkApplication.class, args);
		}

	}
}
