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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.ignite.IgniteCompute;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dexecutor.core.DexecutorState;
import com.github.dexecutor.core.ExecutionEngine;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskExecutionException;
/**
 * Distributed Execution Engine using Ignite
 * 
 * @author Nadeem Mohammad
 *
 * @param <T> Type of Node/Task ID
 * @param <R> Type of Node/Task result
 */
public final class IgniteExecutionEngine<T extends Comparable<T>, R> implements ExecutionEngine<T, R> {

	private static final Logger logger = LoggerFactory.getLogger(IgniteExecutionEngine.class);

	private IgniteCompute igniteCompute;
	private BlockingQueue<ExecutionResult<T,R>> completionQueue;
	private final DexecutorState<T, R> dexecutorState;

	public IgniteExecutionEngine(final DexecutorState<T, R> dexecutorState, final IgniteCompute igniteCompute) {
		this(dexecutorState, igniteCompute, new LinkedBlockingQueue<Future<ExecutionResult<T,R>>>());
	}

	public IgniteExecutionEngine(final DexecutorState<T, R> dexecutorState, final IgniteCompute igniteCompute, BlockingQueue<Future<ExecutionResult<T,R>>> completionQueue) {
		checkNotNull(igniteCompute, "Executer Service should not be null");		
		checkNotNull(completionQueue, "BlockingQueue should not be null");
		this.dexecutorState = dexecutorState;
		this.igniteCompute = igniteCompute.withAsync();
		this.completionQueue = new LinkedBlockingQueue<>();
	}

	@Override
	public void submit(final Task<T, R> task) {
		logger.debug("Received Task {}",  task.getId());
		this.igniteCompute.call(new SerializableCallable<T, R>(task));
		igniteCompute.future().listen(newListener());
	}

	private IgniteInClosure<IgniteFuture<Object>> newListener() {
		return new IgniteInClosure<IgniteFuture<Object>>() {

			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unchecked")
			@Override
			public void apply(IgniteFuture<Object> e) {
				completionQueue.add((ExecutionResult<T, R>) e.get());				
			}			
        };
	}

	@Override
	public ExecutionResult<T, R> processResult() throws TaskExecutionException {

		ExecutionResult<T, R> executionResult;
		try {
			executionResult = completionQueue.take();
			if (executionResult.isSuccess()) {
				this.dexecutorState.removeErrored(executionResult);
			} else {
				this.dexecutorState.addErrored(executionResult);
			}
			return executionResult;
		} catch (InterruptedException e) {
			throw new TaskExecutionException("Task interrupted");
		}		
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

	@Override
	public boolean isAnyTaskInError() {
		return this.dexecutorState.erroredCount() > 0;
	}

}
