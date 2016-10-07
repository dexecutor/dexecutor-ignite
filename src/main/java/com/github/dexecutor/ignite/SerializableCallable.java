package com.github.dexecutor.ignite;

import org.apache.ignite.lang.IgniteCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.ExecutionStatus;
import com.github.dexecutor.core.task.Task;

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
