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

import java.util.Collections;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.monitor.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.monitor.Message.MessageProperties;
import org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.CursorType;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * A simple factory for creating {@link Task} for a given {@link SubscriptionRequest}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
class TaskFactory {

	private final MongoTemplate tempate;

	public TaskFactory(MongoTemplate template) {
		this.tempate = template;
	}

	/**
	 * Create a {@link Task} for the given {@link SubscriptionRequest}.
	 *
	 * @param request must not be {@literal null}.
	 * @return must not be {@literal null}. Consider {@code Object.class}.
	 * @throws IllegalArgumentException in case the {@link SubscriptionRequest} is unknown.
	 */
	Task forRequest(SubscriptionRequest<?, ?> request, Class<?> targetType) {

		Assert.notNull(request, "Request must not be null!");
		Assert.notNull(targetType, "TargetType must not be null!");

		if (request instanceof ChangeStreamRequest) {
			return new ChangeStreamTask(tempate, (ChangeStreamRequest) request, targetType);
		} else if (request instanceof TailableCursorRequest) {
			return new TailableCursorTask(tempate, (TailableCursorRequest) request, targetType);
		}

		throw new IllegalArgumentException(
				"oh wow - seems you're using some fancy new feature we do not support. Please be so kind and leave us a note in the issue tracker so we can get this fixed.\nThank you!");
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	abstract static class CursorReadingTask<T> implements Task {

		private final Object lifecycleMonitor = new Object();

		private final SubscriptionRequest request;
		private final MongoTemplate template;
		private final Class<?> targetType;

		private State state = State.CREATED;

		private MongoCursor<T> cursor;

		public CursorReadingTask(MongoTemplate template, SubscriptionRequest request, Class<?> targetType) {

			this.template = template;
			this.request = request;
			this.targetType = targetType;
		}

		@Override
		public void run() {

			start();

			while (State.ACTIVE.equals(state)) {

				try {
					if (cursor.hasNext()) { // todo need to check if the cursor is still open to avoid errors on read.
						request.getMessageListener()
								.onMessage(createMessage(cursor.next(), targetType, request.getRequestOptions()));
					}
				} catch (IllegalStateException e) {
					System.out.println("damnit error in close : " + e.getMessage());
				}
			}
		}

		private void start() {

			synchronized (lifecycleMonitor) {
				if (!State.ACTIVE.equals(state)) {
					state = State.STARTING;
				}
			}

			boolean valid = false;

			do {

				synchronized (lifecycleMonitor) {

					if (State.STARTING.equals(state)) {

						MongoCursor<T> tmp = initCursor(template, request.getRequestOptions());
						valid = isValidCursor(tmp);
						if (valid) {
							cursor = tmp;
							state = State.ACTIVE;
						} else {
							tmp.close();
						}
					}
				}

				if (!valid) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						Thread.interrupted();
					}
				}
			} while (!valid);
		}

		private boolean isValidCursor(MongoCursor<?> cursor) {

			if (cursor == null) {
				return false;
			}

			if (cursor.getServerCursor() == null || cursor.getServerCursor().getId() == 0) {
				return false;
			}

			return true;
		}

		protected abstract MongoCursor<T> initCursor(MongoTemplate dbFactory, RequestOptions options);

		private Message createMessage(T source, Class targetType, RequestOptions options) {
			return new LazyMappingDelegatingMessage(doCreateMessage(source, options), targetType, template.getConverter());
		}

		protected Message doCreateMessage(T source, RequestOptions options) {
			return new SimpleMessage(source, source,
					MessageProperties.builder().collectionName(options.getCollectionName()).build());
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
	static class ChangeStreamTask extends CursorReadingTask<ChangeStreamDocument<Document>> {

		private final QueryMapper queryMapper;

		ChangeStreamTask(MongoTemplate template, ChangeStreamRequest request, Class<?> targetType) {
			super(template, request, targetType);

			queryMapper = new QueryMapper(template.getConverter());
		}

		@Override
		protected MongoCursor<ChangeStreamDocument<Document>> initCursor(MongoTemplate template, RequestOptions options) {

			List<Document> filter = Collections.emptyList();
			BsonDocument resumeToken = new BsonDocument();

			if (options instanceof ChangeStreamRequestOptions) {

				ChangeStreamRequestOptions changeStreamRequestOptions = (ChangeStreamRequestOptions) options;
				filter = prepareFilter(template, changeStreamRequestOptions);

				if (changeStreamRequestOptions.getResumeToken().isPresent()) {
					resumeToken = BsonDocument.parse(changeStreamRequestOptions.getResumeToken().get().toJson());
				}
			}

			ChangeStreamIterable<Document> iterable = filter.isEmpty()
					? template.getCollection(options.getCollectionName()).watch(Document.class)
					: template.getCollection(options.getCollectionName()).watch(filter, Document.class);

			if (!resumeToken.isEmpty()) {
				iterable = iterable.resumeAfter(resumeToken);
			}

			return iterable.iterator();
		}

		List<Document> prepareFilter(MongoTemplate template, ChangeStreamRequestOptions options) {

			if (options.getFilter().isPresent()) {

				Aggregation agg = options.getFilter().get();
				AggregationOperationContext context = agg instanceof TypedAggregation
						? new TypeBasedAggregationOperationContext(((TypedAggregation) agg).getInputType(),
								template.getConverter().getMappingContext(), queryMapper)
						: Aggregation.DEFAULT_CONTEXT;

				return agg.toPipeline(context);
			}

			return Collections.emptyList();
		}

		@Override
		protected Message doCreateMessage(ChangeStreamDocument<Document> source, RequestOptions options) {
			return new SimpleMessage(source, source.getFullDocument(),
					MessageProperties.builder().collectionName(options.getCollectionName()).build());
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class TailableCursorTask extends CursorReadingTask<Document> {

		public TailableCursorTask(MongoTemplate template, TailableCursorRequest request, Class<?> targetType) {
			super(template, request, targetType);
		}

		@Override
		protected MongoCursor<Document> initCursor(MongoTemplate template, RequestOptions options) {
			return template.getCollection(options.getCollectionName()).find().cursorType(CursorType.TailableAwait)
					.noCursorTimeout(true).iterator();
		}

	}

	static class LazyMappingDelegatingMessage<S, T> implements Message<S, T> {

		private final Message<S, ?> delegate;
		private final Class<T> targetType;
		private final MongoConverter converter;

		public LazyMappingDelegatingMessage(Message<S, ?> delegate, Class<T> targetType, MongoConverter converter) {

			this.delegate = delegate;
			this.targetType = targetType;
			this.converter = converter;
		}

		@Nullable
		@Override
		public S getRaw() {
			return delegate.getRaw();
		}

		@Override
		public T getBody() {

			if (delegate.getBody() == null || targetType.equals(delegate.getBody().getClass())) {
				return targetType.cast(delegate.getBody());
			}

			Object messageBody = delegate.getBody();

			if (ClassUtils.isAssignable(Document.class, messageBody.getClass())) {
				return converter.read(targetType, (Document) messageBody);
			}

			if (converter.getConversionService().canConvert(messageBody.getClass(), targetType)) {
				return converter.getConversionService().convert(messageBody, targetType);
			}

			throw new IllegalArgumentException(
					String.format("No converter found capable of converting %s to %s", messageBody.getClass(), targetType));
		}

		@Override
		public MessageProperties getMessageProperties() {
			return delegate.getMessageProperties();
		}
	}
}
