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

import org.springframework.context.SmartLifecycle;

/**
 * Internal abstraction used by the framework representing a message listener container. <strong>Not</strong> meant to
 * be implemented externally.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
interface MessageListenerContainer extends SmartLifecycle {

	/**
	 * Register a new {@link SubscriptionRequest} in the container. If the {@link MessageListenerContainer#isRunning() is
	 * already running} the subscription will be added and run immediately, otherwise it'll be scheduled and started once
	 * the container is actually {@link MessageListenerContainer#start() started}.
	 * <p />
	 * On {@link MessageListenerContainer#stop()} all {@link Subscription subscriptions} are cancelled prior to shutting
	 * down the container itself.
	 * <p />
	 * Registering the very same {@link SubscriptionRequest} more than once simply returns the already existing
	 * {@link Subscription}.
	 * <p />
	 * Unless a {@link Subscription} is {@link #remove(Subscription) removed} form the container, the {@link Subscription}
	 * is restarted once the container itself is restarted.
	 *
	 * @param request must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	Subscription register(SubscriptionRequest request);

	/**
	 * Unregister a given {@link Subscription} from the container. This prevents the {@link Subscription} to be restarted
	 * in a potential {@link SmartLifecycle#stop() stop}/{@link SmartLifecycle#start() start} scenario.<br />
	 * {@link Subscription#isActive() Active} {@link Subscription subcriptions} are {@link Subscription#cancel()
	 * cancelled} prior to removal.
	 *
	 * @param subscription must not be {@literal null}.
	 */
	void remove(Subscription subscription);
}
