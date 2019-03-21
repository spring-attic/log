/*
 * Copyright 2015 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
@SpringBootTest({"server.port=-1", "log.name=foo", "log.level=warn",
		"log.expression=payload.toUpperCase()"})
public class LogSinkApplicationTests {

	@Autowired
	@Bindings(LogSinkConfiguration.class)
	private Sink sink;

	@SuppressWarnings("unused")
	@Autowired
	private Sink same;

	@Autowired
	private LoggingHandler logSinkHandler;

	@Test
	public void test() {
		assertNotNull(this.sink.input());
		assertEquals(LoggingHandler.Level.WARN, this.logSinkHandler.getLevel());
		Log logger = TestUtils.getPropertyValue(this.logSinkHandler, "messageLogger",
				Log.class);
		assertEquals("foo", TestUtils.getPropertyValue(logger, "name"));
		logger = spy(logger);
		new DirectFieldAccessor(this.logSinkHandler).setPropertyValue("messageLogger",
				logger);
		GenericMessage<String> message = new GenericMessage<>("foo");
		this.sink.input().send(message);
		ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
		verify(logger).warn(captor.capture());
		assertEquals("FOO", captor.getValue());
		this.logSinkHandler.setExpression("#this");
		this.sink.input().send(message);
		verify(logger, times(2)).warn(captor.capture());
		assertSame(message, captor.getValue());
	}


	@SpringBootApplication
	static class LogSinkApplication {

		public static void main(String[] args) {
			SpringApplication.run(LogSinkApplication.class, args);
		}

	}


}
