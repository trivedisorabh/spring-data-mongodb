/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.monitor;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.monitor.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions;
import org.springframework.data.mongodb.monitor.TaskFactory.ChangeStreamTask;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import org.springframework.data.mongodb.monitor.TaskFactory.TailableCursorTask;

/**
 * Unit tests for {@link TaskFactory}.
 *
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class TaskFactoryUnitTests {

	@Mock MongoConverter converter;
	@Mock MongoTemplate template;
	@Mock MessageListener<Message> messageListener;

	TaskFactory factory;

	@Before
	public void setUp() {

		when(template.getConverter()).thenReturn(converter);
		factory = new TaskFactory(template);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1803
	public void requestMustNotBeNull() {
		factory.forRequest(null, Object.class);
	}

	@Test // DATAMONGO-1803
	public void createsChangeStreamRequestCorrectly() {

		ChangeStreamRequestOptions options = Mockito.mock(ChangeStreamRequestOptions.class);
		Task task = factory.forRequest(new ChangeStreamRequest(messageListener, options), Object.class);

		assertThat(task).isInstanceOf(ChangeStreamTask.class);
	}

	@Test // DATAMONGO-1803
	public void createstailableRequestCorrectly() {

		RequestOptions options = Mockito.mock(RequestOptions.class);
		Task task = factory.forRequest(new TailableCursorRequest(messageListener, options), Object.class);

		assertThat(task).isInstanceOf(TailableCursorTask.class);
	}
}
