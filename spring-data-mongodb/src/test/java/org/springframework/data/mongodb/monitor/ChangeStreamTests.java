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

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.monitor.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.monitor.Message.ConvertibleMessage;
import org.springframework.data.mongodb.test.util.Assertions;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;

/**
 * @author Christoph Strobl
 */
public class ChangeStreamTests {

	public static final String DATABASE_NAME = "change-stream-events";
	public static final String COLLECTION_NAME = "collection-1";
	MongoDbFactory dbFactory;

	MongoCollection<Document> collection;
	private MongoDatabase db;
	private CollectingMessageListener messageListener;

	// TODO: add class rule verifying DB is running in replica set or a sharded cluster mode.

	@Before
	public void setUp() {

		dbFactory = new SimpleMongoDbFactory(new MongoClient(), DATABASE_NAME);
		db = dbFactory.getDb(DATABASE_NAME);
		collection = db.getCollection(COLLECTION_NAME);

		collection.drop();

		messageListener = new CollectingMessageListener();
	}

	@Test // DATAMONGO-1803
	public void shouldOnlyReceiveMessagesWhileActive() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(dbFactory);
		container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME));
		container.start();

		Thread.sleep(200);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		Thread.sleep(200);

		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));

		Thread.sleep(200);

		Assertions.assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	public void mapping() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(dbFactory);

		MongoTemplate template = new MongoTemplate(dbFactory);

		// TODO: find a better way for messages and conversion.
		container.register(new ChangeStreamRequest(messageListener, ChangeStreamRequestOptions.builder()
				.collection(COLLECTION_NAME).converter(MessageConverter.mapping(template.getConverter())).build()));
		container.start();

		Thread.sleep(200);

		collection.insertOne(new Document("_id", "id-1").append("firstname", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("lastname", "bar"));

		Thread.sleep(200);

		container.stop();

		List<ConvertibleMessage> messages = (List<ConvertibleMessage>) messageListener.getMessages().stream()
				.filter((val) -> val instanceof Message.ConvertibleMessage).map(ConvertibleMessage.class::cast)
				.collect(Collectors.toList());

		messages.forEach(val -> System.out.println(val.getConverted(Person.class)));
	}

	@Test
	public void startAndListenToChangeStream() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(dbFactory);
		container.start();
		container.register(new ChangeStreamRequest(System.out::println, () -> COLLECTION_NAME));

		Thread.sleep(200);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		Thread.sleep(1000);
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		Thread.sleep(1000);
		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));
		Thread.sleep(2000);
		collection.insertOne(new Document("_id", "id-4").append("value", "bar"));
		Thread.sleep(2000);
	}

	@Test
	public void startAndListenToChangeStreamOther() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(dbFactory);
		container.register(new ChangeStreamRequest(System.out::println, () -> COLLECTION_NAME));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		Thread.sleep(1000);

		container.start();

		Thread.sleep(500);

		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		Thread.sleep(1000);
		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));
		Thread.sleep(2000);
	}

	@Test
	public void testInfiniteStreams() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(dbFactory);
		container.register(new TailableCursorRequest(System.out::println, () -> COLLECTION_NAME));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		Thread.sleep(1000);

		container.start();

		Thread.sleep(500);

		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		Thread.sleep(1000);
		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));
		Thread.sleep(2000);
	}

	static class CollectingMessageListener<M extends Message> implements MessageListener<M> {

		List<M> messages = new ArrayList<>();

		@Override
		public void onMessage(M message) {
			messages.add(message);
		}

		int getTotalNumberMessagesReceived() {
			return messages.size();
		}

		public List<M> getMessages() {
			return messages;
		}
	}

	@Data
	static class Person {
		@Id String id;
		private String firstname;
		private String lastname;
	}
}
