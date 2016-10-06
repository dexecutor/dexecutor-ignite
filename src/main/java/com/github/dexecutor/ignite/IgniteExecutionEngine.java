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

import static com.github.dexecutor.core.support.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dexecutor.core.ExecutionEngine;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskExecutionException;
/**
 * Distributed Execution Engine using Ignite
 * 
 * @author Nadeem Mohammad
 *
 * @param <T>
 * @param <R>
 */
public final class IgniteExecutionEngine<T extends Comparable<T>, R> implements ExecutionEngine<T, R> {

	private static final Logger logger = LoggerFactory.getLogger(IgniteExecutionEngine.class);
	private Collection<T> erroredTasks = new CopyOnWriteArraySet<T>();

	private final ExecutorService executorService;
	//Need Completion service implementation by Ignite.
	private final CompletionService<ExecutionResult<T,R>> completionService;

	public IgniteExecutionEngine(final ExecutorService executorService) {
		checkNotNull(executorService, "Executer Service should not be null");
		this.executorService = executorService;
		this.completionService = new ExecutorCompletionService<ExecutionResult<T,R>>(executorService);
	}

	@Override
	public void submit(Task<T, R> task) {
		logger.debug("Received Task {} ", task.getId());
		this.completionService.submit(new SerializableCallable<T, R>(task));			
	}

	@Override
	public ExecutionResult<T, R> processResult() throws TaskExecutionException {

		ExecutionResult<T, R> executionResult;
		try {
			executionResult = completionService.take().get();
			if (executionResult.isSuccess()) {				
				erroredTasks.remove(executionResult.getId());
			} else {
				erroredTasks.add(executionResult.getId());
			}
			return executionResult;
		} catch (InterruptedException | ExecutionException e) {
			throw new TaskExecutionException("Task interrupted");
		}		
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

	@Override
	public boolean isAnyTaskInError() {
		return !this.erroredTasks.isEmpty();
	}

	@Override
	public String toString() {
		return this.executorService.toString();
	}
}
