package com.github.dexecutor.ignite;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.Task;

public class SerializableCallable <T extends Comparable<T>, R> implements Callable<ExecutionResult<T,R>>, Serializable {

	private static final long serialVersionUID = 1L;
	private Task<T, R> task;

	public SerializableCallable(Task<T, R> task) {
		this.task = task;
	}

	@Override
	public ExecutionResult<T, R> call() throws Exception {
		R r = task.execute();
		return new ExecutionResult<T, R>(task.getId(), r, task.getStatus());
	}
}
