/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.dexecutor.ignite;

import org.apache.ignite.lang.IgniteCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.ExecutionStatus;
import com.github.dexecutor.core.task.Task;
/**
 * 
 * @author Nadeem Mohammad
 *
 * @param <T> Type of Node/Task ID
 * @param <R> Type of Node/Task result
 */
public class SerializableCallable <T extends Comparable<T>, R> implements IgniteCallable<ExecutionResult<T,R>> {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(SerializableCallable.class);

	private Task<T, R> task;

	public SerializableCallable(Task<T, R> task) {
		this.task = task;
	}

	@Override
	public ExecutionResult<T, R> call() throws Exception {
		R r = null;
		ExecutionStatus status = ExecutionStatus.SUCCESS;
		try {
			r = task.execute();
		} catch (Exception e) {
			status = ExecutionStatus.ERRORED;
			logger.error("Error Execution Task # {}", task.getId(), e);
		}
		return new ExecutionResult<T, R>(task.getId(), r, status);
	}
}
