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
import lombok.ToString;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * General message abstraction for any type of Event / Message published by MongoDB server to the client. This might be
 * <a href="https://docs.mongodb.com/manual/reference/change-events/">Change Stream Events</a>, or
 * {@link org.bson.Document Documents} published by a
 * <a href="https://docs.mongodb.com/manual/core/tailable-cursors/">tailable cursor</a>. The original message received
 * is preserved in the raw parameter. Additional information about the origin of the {@link Message} is contained in
 * {@link MessageProperties}. <br />
 * For convenience the {@link #getBody()} of the message gets lazily converted into the target domain type if necessary
 * using the mapping infrastructure.
 *
 * @author Christoph Strobl
 * @see MessageProperties
 * @since 2.1
 */
interface Message<S, T> {

	/**
	 * The raw message source as emitted by the origin.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	S getRaw();

	/**
	 * The converted message body if applicable.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	T getBody();

	/**
	 * {@link MessageProperties} containing information about the {@link Message} origin and other metadata.
	 *
	 * @return never {@literal null}.
	 */
	MessageProperties getMessageProperties();

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@ToString
	@EqualsAndHashCode
	static class MessageProperties {

		private static final MessageProperties EMPTY = new MessageProperties();

		private String databaseName;
		private String collectionName;

		/**
		 * The database name the message originates from.
		 *
		 * @return
		 */
		@Nullable
		public String getDatabaseName() {
			return databaseName;
		}

		/**
		 * The collection name the message originates from.
		 *
		 * @return
		 */
		@Nullable
		public String getCollectionName() {
			return collectionName;
		}

		/**
		 * @return empty {@link MessageProperties}.
		 */
		public static MessageProperties empty() {
			return EMPTY;
		}

		/**
		 * Obtain a shiny new {@link MessagePropertiesBuilder} and start defining options in this fancy fluent way. Just
		 * don't forget to call {@link MessagePropertiesBuilder#build() build()} when your're done.
		 *
		 * @return new instance of {@link MessagePropertiesBuilder}.
		 */
		public static MessagePropertiesBuilder builder() {
			return new MessagePropertiesBuilder();
		}

		/**
		 * @author Christoph Strobl
		 * @since 2.1
		 */
		public static class MessagePropertiesBuilder {

			private MessageProperties properties = new MessageProperties();

			MessagePropertiesBuilder databaseName(String dbName) {

				Assert.notNull(dbName, "DbName must not be null!");

				properties.databaseName = dbName;
				return this;
			}

			MessagePropertiesBuilder collectionName(String collectionName) {

				Assert.notNull(collectionName, "CollectionName must not be null!");

				properties.collectionName = collectionName;
				return this;
			}

			MessageProperties build() {

				MessageProperties properties = this.properties;
				this.properties = new MessageProperties();
				return properties;
			}
		}
	}
}
