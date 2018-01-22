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

import org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions;

/**
 * {@link SubscriptionRequest} already proving the task to execute.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public interface TaskSubscriptionRequest<M extends Message, O extends RequestOptions>
		extends SubscriptionRequest<M, O> {

	/**
	 * Get the {@link Task} to run.
	 *
	 * @return never {@literal null}.
	 */
	Task getTask();

}
