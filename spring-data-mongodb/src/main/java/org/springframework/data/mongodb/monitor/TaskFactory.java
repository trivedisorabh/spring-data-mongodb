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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.aggregation.Field;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.monitor.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.monitor.Message.MessageProperties;
import org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions;
import org.springframework.data.mongodb.monitor.TailableCursorRequest.TailableCursorRequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ErrorHandler;

import com.mongodb.CursorType;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

/**
 * A simple factory for creating {@link Task} for a given {@link SubscriptionRequest}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
class TaskFactory {

	private final MongoTemplate tempate;

	/**
	 * @param template must not be {@literal null}.
	 */
	TaskFactory(MongoTemplate template) {

		Assert.notNull(template, "Template must not be null!");
		this.tempate = template;
	}

	/**
	 * Create a {@link Task} for the given {@link SubscriptionRequest}.
	 *
	 * @param request must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @param errorHandler must not be {@literal null}.
	 * @return must not be {@literal null}. Consider {@code Object.class}.
	 * @throws IllegalArgumentException in case the {@link SubscriptionRequest} is unknown.
	 */
	Task forRequest(SubscriptionRequest<?, ?> request, Class<?> targetType, ErrorHandler errorHandler) {

		Assert.notNull(request, "Request must not be null!");
		Assert.notNull(targetType, "TargetType must not be null!");

		if (request instanceof ChangeStreamRequest) {
			return new ChangeStreamTask(tempate, (ChangeStreamRequest) request, targetType, errorHandler);
		} else if (request instanceof TailableCursorRequest) {
			return new TailableCursorTask(tempate, (TailableCursorRequest) request, targetType, errorHandler);
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

		private final ErrorHandler errorHandler;

		/**
		 * @param template must not be {@literal null}.
		 * @param request must not be {@literal null}.
		 * @param targetType must not be {@literal null}.
		 */
		public CursorReadingTask(MongoTemplate template, SubscriptionRequest request, Class<?> targetType,
				ErrorHandler errorHandler) {

			this.template = template;
			this.request = request;
			this.targetType = targetType;
			this.errorHandler = errorHandler;
		}

		/* 
		 * (non-Javadoc)
		 * @see java.lang.Runnable
		 */
		@Override
		public void run() {

			start();

			while (isRunning()) {
				try {
					T next = getNext();
					if (next != null) {
						emitMessage(createMessage(next, targetType, request.getRequestOptions()));
					} else {
						Thread.sleep(10);
					}
				} catch (InterruptedException e) {
					Thread.interrupted();
				} catch (Exception e) {

					Exception toHandle = e;

					if (e instanceof RuntimeException) {

						Exception translated = template.getExceptionTranslator().translateExceptionIfPossible((RuntimeException) e);
						toHandle = translated != null ? translated : e;
					}

					errorHandler.handleError(toHandle);
				}
			}
		}

		/**
		 * Initialize the Task by 1st setting the current state to
		 * {@link org.springframework.data.mongodb.monitor.Task.State#STARTING starting} indicating the initialization
		 * procedure. <br />
		 * Moving on the underlying {@link MongoCursor} gets {@link #initCursor(MongoTemplate, RequestOptions) created} and
		 * is {@link #isValidCursor(MongoCursor) health checked}. Once a valid {@link MongoCursor} is created the
		 * {@link #state} is set to {@link org.springframework.data.mongodb.monitor.Task.State#RUNNING running}. If the
		 * health check is not passed the {@link MongoCursor} is immediately {@link MongoCursor#close() closed} and a new
		 * {@link MongoCursor} is requested until a valid one is retrieved or the {@link #state} changes.
		 */
		private void start() {

			synchronized (lifecycleMonitor) {
				if (!State.RUNNING.equals(state)) {
					state = State.STARTING;
				}
			}

			do {

				boolean valid = false;

				synchronized (lifecycleMonitor) {

					if (State.STARTING.equals(state)) {

						MongoCursor<T> tmp = initCursor(template, request.getRequestOptions(), targetType);
						valid = isValidCursor(tmp);
						if (valid) {
							cursor = tmp;
							state = State.RUNNING;
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
			} while (State.STARTING.equals(getState()));
		}

		protected abstract MongoCursor<T> initCursor(MongoTemplate template, RequestOptions options, Class<?> targetType);

		@Override
		public void cancel() throws DataAccessResourceFailureException {

			synchronized (lifecycleMonitor) {

				if (State.RUNNING.equals(state) || State.STARTING.equals(state)) {
					this.state = State.CANCELLED;
					if (cursor != null) {
						cursor.close();
					}
				}
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public State getState() {

			synchronized (lifecycleMonitor) {
				return state;
			}
		}

		private Message createMessage(T source, Class targetType, RequestOptions options) {
			return new LazyMappingDelegatingMessage(doCreateMessage(source, options), targetType, template.getConverter());
		}

		/**
		 * Customization hook.
		 *
		 * @param source never {@literal null}.
		 * @param options never {@literal null}.
		 * @return never {@literal null}.
		 */
		protected Message doCreateMessage(T source, RequestOptions options) {
			return new SimpleMessage(source, source, MessageProperties.builder().databaseName(template.getDb().getName())
					.collectionName(options.getCollectionName()).build());
		}

		private boolean isRunning() {
			return State.RUNNING.equals(getState());
		}

		private void emitMessage(Message message) {
			request.getMessageListener().onMessage(message);
		}

		private T getNext() {

			synchronized (lifecycleMonitor) {
				if (State.RUNNING.equals(state)) {
					return cursor.tryNext();
				}
			}

			throw new IllegalStateException(String.format("Cursor %s is not longer open.", cursor));
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
	}

	/**
	 * {@link Task} implementation for obtaining {@link ChangeStreamDocument ChangeStreamDocuments} from MongoDB.
	 * 
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class ChangeStreamTask extends CursorReadingTask<ChangeStreamDocument<Document>> {

		private final QueryMapper queryMapper;

		ChangeStreamTask(MongoTemplate template, ChangeStreamRequest request, Class<?> targetType,
				ErrorHandler errorHandler) {
			super(template, request, targetType, errorHandler);

			queryMapper = new QueryMapper(template.getConverter());
		}

		@Override
		protected MongoCursor<ChangeStreamDocument<Document>> initCursor(MongoTemplate template, RequestOptions options,
				Class<?> targetType) {

			List<Document> filter = Collections.emptyList();
			BsonDocument resumeToken = new BsonDocument();
			Collation collation = null;
			FullDocument fullDocument = FullDocument.DEFAULT;

			if (options instanceof ChangeStreamRequestOptions) {

				ChangeStreamRequestOptions changeStreamRequestOptions = (ChangeStreamRequestOptions) options;
				filter = prepareFilter(template, changeStreamRequestOptions);

				if (changeStreamRequestOptions.getFilter().isPresent()) {

					Object val = changeStreamRequestOptions.getFilter().get();
					if (val instanceof Aggregation) {
						collation = ((Aggregation) val).getOptions().getCollation()
								.map(org.springframework.data.mongodb.core.query.Collation::toMongoCollation).orElse(null);
					}
				}

				if (changeStreamRequestOptions.getResumeToken().isPresent()) {
					resumeToken = changeStreamRequestOptions.getResumeToken().get().asDocument();
				}

				fullDocument = changeStreamRequestOptions.getFullDocumentLookup()
						.orElseGet(() -> ClassUtils.isAssignable(Document.class, targetType) ? FullDocument.DEFAULT
								: FullDocument.UPDATE_LOOKUP);
			}

			ChangeStreamIterable<Document> iterable = filter.isEmpty()
					? template.getCollection(options.getCollectionName()).watch(Document.class)
					: template.getCollection(options.getCollectionName()).watch(filter, Document.class);

			if (!resumeToken.isEmpty()) {
				iterable = iterable.resumeAfter(resumeToken);
			}

			if (collation != null) {
				iterable = iterable.collation(collation);
			}

			iterable = iterable.fullDocument(fullDocument);

			return iterable.iterator();
		}

		List<Document> prepareFilter(MongoTemplate template, ChangeStreamRequestOptions options) {

			if (!options.getFilter().isPresent()) {
				return Collections.emptyList();
			}

			Object filter = options.getFilter().get();
			if (filter instanceof Aggregation) {
				Aggregation agg = (Aggregation) filter;
				AggregationOperationContext context = agg instanceof TypedAggregation
						? new TypeBasedAggregationOperationContext(((TypedAggregation) agg).getInputType(),
								template.getConverter().getMappingContext(), queryMapper)
						: Aggregation.DEFAULT_CONTEXT;

				return agg.toPipeline(new PrefixingDelegatingAggregationOperationContext(context));
			} else if (filter instanceof List) {
				return (List<Document>) filter;
			} else {
				throw new IllegalArgumentException(
						"ChangeStreamRequestOptions.filter mut be either an Aggregation or a plain list of Documents");
			}
		}

		@Override
		protected Message doCreateMessage(ChangeStreamDocument<Document> source, RequestOptions options) {

			// namespace might be null for eg. OperationType.INVALIDATE
			MongoNamespace namespace = Optional.ofNullable(source.getNamespace())
					.orElse(new MongoNamespace("unknown", "unknown"));

			return new SimpleMessage(source, source.getFullDocument(), MessageProperties.builder()
					.databaseName(namespace.getDatabaseName()).collectionName(namespace.getCollectionName()).build());
		}

		static class PrefixingDelegatingAggregationOperationContext implements AggregationOperationContext {

			final AggregationOperationContext delegate;

			public PrefixingDelegatingAggregationOperationContext(AggregationOperationContext delegate) {
				this.delegate = delegate;
			}

			@Override
			public Document getMappedObject(Document document) {
				return prefix(delegate.getMappedObject(document));
			}

			private Document prefix(Document source) {

				Document result = new Document();
				for (Map.Entry<String, Object> entry : source.entrySet()) {

					String key = entry.getKey().startsWith("$") ? entry.getKey() : "fullDocument." + entry.getKey();
					Object value = entry.getValue();

					if (entry.getValue() instanceof Collection) {
						List tmp = new ArrayList();
						for (Object o : (Collection) entry.getValue()) {
							if (o instanceof Document) {
								tmp.add(prefix((Document) o));
							} else {
								tmp.add(o);
							}
						}
						value = tmp;
					} else if (entry.getValue() instanceof Document) {
						value = prefix((Document) entry.getValue());
					}

					result.append(key, value);
				}
				return result;
			}

			@Override
			public FieldReference getReference(Field field) {
				return delegate.getReference(field);
			}

			@Override
			public FieldReference getReference(String name) {
				return delegate.getReference(name);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class TailableCursorTask extends CursorReadingTask<Document> {

		private QueryMapper queryMapper;

		public TailableCursorTask(MongoTemplate template, TailableCursorRequest request, Class<?> targetType,
				ErrorHandler errorHandler) {
			super(template, request, targetType, errorHandler);
			queryMapper = new QueryMapper(template.getConverter());
		}

		@Override
		protected MongoCursor<Document> initCursor(MongoTemplate template, RequestOptions options, Class<?> targetType) {

			Document filter = new Document();
			Collation collation = null;

			if (options instanceof TailableCursorRequestOptions) {

				TailableCursorRequestOptions requestOptions = (TailableCursorRequestOptions) options;
				if (requestOptions.getQuery().isPresent()) {

					Query query = requestOptions.getQuery().get();

					filter.putAll(queryMapper.getMappedObject(query.getQueryObject(), template.getConverter().getMappingContext()
							.getPersistentEntity(targetType.equals(Document.class) ? Object.class : targetType)));

					collation = query.getCollation().map(org.springframework.data.mongodb.core.query.Collation::toMongoCollation)
							.orElse(null);
				}
			}

			FindIterable<Document> iterable = template.getCollection(options.getCollectionName()).find(filter)
					.cursorType(CursorType.TailableAwait).noCursorTimeout(true);

			if (collation != null) {
				iterable = iterable.collation(collation);
			}

			return iterable.iterator();
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

		@Override
		public String toString() {
			return "LazyMappingDelegatingMessage {" + "delegate=" + delegate + ", targetType=" + targetType + '}';
		}
	}
}
