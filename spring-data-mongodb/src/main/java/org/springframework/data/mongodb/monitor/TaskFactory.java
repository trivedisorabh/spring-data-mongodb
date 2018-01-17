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

import org.bson.Document;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.monitor.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.monitor.Message.MessageProperties;
import org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions;
import org.springframework.util.Assert;

import com.mongodb.CursorType;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * A simple factory for creating {@link Task} for a given {@link SubscriptionRequest}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
class TaskFactory {

	private final MongoDbFactory dbFactory;

	public TaskFactory(MongoDbFactory dbFactory) {
		this.dbFactory = dbFactory;
	}

	/**
	 * Create a {@link Task} for the given {@link SubscriptionRequest}.
	 *
	 * @param request must not be {@literal null}.
	 * @return must not be {@literal null}.
	 * @throws IllegalArgumentException in case the {@link SubscriptionRequest} is unknown.
	 */
	Task forRequest(SubscriptionRequest<?, ?> request) {

		Assert.notNull(request, "Request must not be null!");

		if (request instanceof ChangeStreamRequest) {
			return new ChangeStreamTask(dbFactory, (ChangeStreamRequest) request);
		} else if (request instanceof TailableCursorRequest) {
			return new TailableCursorTask(dbFactory, (TailableCursorRequest) request);
		}

		throw new IllegalArgumentException(
				"oh wow - seems you're using some fancy new feature we do not support. Please be so kind and leave us a note in the issue tracker so we can get this fixed.\nThank you!");
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	private abstract static class CursorReadingTask<T> implements Task {

		private final Object lifecycleMonitor = new Object();

		private final SubscriptionRequest request;
		private final MongoDbFactory factory;

		private State state = State.CREATED;

		private MongoCursor<T> cursor;

		public CursorReadingTask(MongoDbFactory factory, SubscriptionRequest request) {

			this.factory = factory;
			this.request = request;
		}

		@Override
		public void run() {

			synchronized (lifecycleMonitor) {

				if (!State.ACTIVE.equals(state)) {
					cursor = initCursor(factory, request.getRequestOptions());
					state = State.ACTIVE;
				}
			}

			while (State.ACTIVE.equals(state)) {
				try {
					request.getMessageListener().onMessage(createMessage(cursor.next(), request.getRequestOptions()));
				} catch (IllegalStateException e) {
					System.out.println("damnit error in close : " + e.getMessage());
				}
			}
		}

		protected abstract MongoCursor<T> initCursor(MongoDbFactory dbFactory, RequestOptions options);

		protected Message createMessage(T source, RequestOptions options) {
			return Message.simple(source, MessageProperties.builder().collectionName(options.getCollectionName()).build());
		}

		@Override
		public boolean isActive() {
			return State.ACTIVE.equals(getState());
		}

		@Override
		public void cancel() throws DataAccessResourceFailureException {

			synchronized (lifecycleMonitor) {

				if (State.ACTIVE.equals(state)) {
					this.state = State.CANCELLED;
					cursor.close();
				}
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		public State getState() {
			synchronized (lifecycleMonitor) {
				return state;
			}
		}
	}

	/**
	 * {@link Task} implementation for obtaining {@link ChangeStreamDocument ChangeStreamDocuments} from MongoDB.
	 * 
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	private static class ChangeStreamTask extends CursorReadingTask<ChangeStreamDocument<Document>> {

		ChangeStreamTask(MongoDbFactory factory, ChangeStreamRequest request) {
			super(factory, request);
		}

		@Override
		protected MongoCursor<ChangeStreamDocument<Document>> initCursor(MongoDbFactory dbFactory, RequestOptions options) {
			return dbFactory.getDb().getCollection(options.getCollectionName()).watch(Document.class).iterator();
		}

		@Override
		protected Message createMessage(ChangeStreamDocument<Document> source, RequestOptions options) {

			Message message = super.createMessage(source, options);

			if (options instanceof ChangeStreamRequestOptions) {

				ChangeStreamRequestOptions csro = (ChangeStreamRequestOptions) options;
				if (csro.getConverter() != null) {
					message = Message.convertible(message, csro.getConverter());
				}
			}

			return message;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	private static class TailableCursorTask extends CursorReadingTask<Document> {

		public TailableCursorTask(MongoDbFactory factory, TailableCursorRequest request) {
			super(factory, request);
		}

		@Override
		protected MongoCursor<Document> initCursor(MongoDbFactory dbFactory, RequestOptions options) {

			return dbFactory.getDb().getCollection(options.getCollectionName()).find().cursorType(CursorType.TailableAwait)
					.iterator();
		}
	}
}
