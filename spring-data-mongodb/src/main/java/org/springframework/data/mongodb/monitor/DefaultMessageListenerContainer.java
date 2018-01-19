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

import lombok.EqualsAndHashCode;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions;
import org.springframework.util.Assert;

/**
 * Simple {@link Executor} based {@link MessageListenerContainer} implementation. <br />
 *
 * @author Christoph Strobl
 * @since 2.1
 */
class DefaultMessageListenerContainer implements MessageListenerContainer {

	private final MongoTemplate template;
	private final Executor taskExecutor;

	private final Object lifecycleMonitor = new Object();

	private int phase = Integer.MAX_VALUE;
	private boolean running = false;

	private volatile Set<Subscription> subscriptions = new CopyOnWriteArraySet<>();
	private final TaskFactory taskFactory;

	DefaultMessageListenerContainer(MongoTemplate template) {
		this(template, new SimpleAsyncTaskExecutor());
	}

	DefaultMessageListenerContainer(MongoTemplate template, Executor taskExecutor) {

		Assert.notNull(template, "Template must not be null!");
		Assert.notNull(taskExecutor, "TaskExecutor must not be null!");

		this.taskExecutor = taskExecutor;
		this.template = template;
		this.taskFactory = new TaskFactory(template);
	}

	@Override
	public boolean isAutoStartup() {
		return false;
	}

	@Override
	public void stop(Runnable callback) {

		synchronized (this.lifecycleMonitor) {
			stop();
			callback.run();
		}
	}

	@Override
	public void start() {

		synchronized (lifecycleMonitor) {

			if (!this.running) {

				for (Subscription subscription : subscriptions) {

					if (!subscription.isActive()) {
						if (subscription instanceof TaskSubscription) {
							taskExecutor.execute(((TaskSubscription) subscription).getTask());
						}
					}
				}
				running = true;
			}
		}
	}

	@Override
	public void stop() {

		synchronized (lifecycleMonitor) {

			if (this.running) {
				for (Subscription subscription : subscriptions) {
					subscription.cancel();
				}
				running = false;
			}
		}
	}

	@Override
	public boolean isRunning() {

		synchronized (this.lifecycleMonitor) {
			return running;
		}
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	@Override
	public <T, M extends Message<?, ? super T>> Subscription register(SubscriptionRequest<M, ? extends RequestOptions> request,
			Class<T> bodyType) {

		Task task = taskFactory.forRequest(request, bodyType);
		Subscription subscription = new TaskSubscription(task);

		synchronized (lifecycleMonitor) {
			this.subscriptions.add(subscription);
			if (this.running) {
				taskExecutor.execute(task);
			}
		}

		return subscription;
	}

	@Override
	public void remove(Subscription subscription) {

		synchronized (lifecycleMonitor) {

			if (subscriptions.contains(subscription)) {

				if (subscription.isActive()) {
					subscription.cancel();
				}

				subscriptions.remove(subscription);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@EqualsAndHashCode
	static class TaskSubscription implements Subscription {

		private final Task task;

		TaskSubscription(Task task) {
			this.task = task;
		}

		Task getTask() {
			return task;
		}

		@Override
		public boolean isActive() {
			return task.isActive();
		}

		@Override
		public void cancel() throws DataAccessResourceFailureException {
			task.cancel();
		}
	}
}
