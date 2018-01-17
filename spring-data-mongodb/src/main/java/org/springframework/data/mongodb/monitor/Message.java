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

import lombok.ToString;

import org.springframework.lang.Nullable;

/**
 * General message abstraction for any type of Event / Message published by MongoDB server to the client. This might be
 * <a href="https://docs.mongodb.com/manual/reference/change-events/">Change Stream Events</a>, or
 * {@link org.bson.Document Documents} published by a
 * <a href="https://docs.mongodb.com/manual/core/tailable-cursors/">tailable cursor</a>. The original message received
 * is preserved in the raw parameter. Additional information about the origin of the {@link Message} is contained in
 * {@link MessageProperties}. <br />
 * For convenience the raw message source can be transformed using a {@link MessageConverter} by delegating the
 * {@link Message} to {@link ConvertibleMessage}.
 *
 * @author Christoph Strobl
 * @since 2.1
 * @see MessageProperties
 */
interface Message<T> {

	/**
	 * The raw message source as emitted by the origin.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	T getRaw();

	/**
	 * {@link MessageProperties} containing information about the {@link Message} origin and other metadata.
	 * 
	 * @return never {@literal null}
	 */
	MessageProperties getMessageProperties();

	static <T> Message<T> simple(T raw, MessageProperties properties) {
		return new SimpleMessage(raw, properties);
	}

	static <S, T> ConvertibleMessage<S, T> convertible(Message<S> message, MessageConverter converter) {
		return new DelegatingConvertibleMessage(message, converter);
	}

	interface ConvertibleMessage<S, T> extends Message<S> {
		T getConverted(Class<T> type);
	}

	static enum Type {
		CHANGE_STREAM, TAILABLE_CURSOR, UNDEFINED
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@ToString
	static class MessageProperties {

		private static final MessageProperties EMPTY = new MessageProperties();

		private String databaseName;
		private String collectionName;
		private Type messageType = Type.UNDEFINED;

		@Nullable
		public String getDatabaseName() {
			return databaseName;
		}

		@Nullable
		public String getCollectionName() {
			return collectionName;
		}

		public Type getMessageType() {
			return messageType;
		}

		static MessageProperties empty() {
			return EMPTY;
		}

		static MessagePropertiesBuilder builder() {
			return new MessagePropertiesBuilder();
		}

		public static class MessagePropertiesBuilder {

			MessageProperties properties = new MessageProperties();

			MessagePropertiesBuilder databaseName(String dbName) {
				properties.databaseName = dbName;
				return this;
			}

			MessagePropertiesBuilder collectionName(String collectionName) {
				properties.collectionName = collectionName;
				return this;
			}

			MessagePropertiesBuilder messageType(Type messageType) {
				properties.messageType = messageType;
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
