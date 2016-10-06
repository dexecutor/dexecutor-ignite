package com.github.dexecutor.ignite;

import java.util.concurrent.ExecutorService;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import com.github.dexecutor.core.DefaultDependentTasksExecutor;
import com.github.dexecutor.core.DependentTasksExecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;

public class Job {
	
	public void run(boolean isMaster, String nodeName) throws Exception {
		
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setGridName(nodeName); 

		Ignite ignite = Ignition.start(cfg); 
		
		if (isMaster) {
			ExecutorService executorService = ignite.executorService();
			DefaultDependentTasksExecutor<Integer, Integer> dexecutor = newTaskExecutor(executorService);

			buildGraph(dexecutor);
			dexecutor.execute(ExecutionConfig.TERMINATING);
		}

		System.out.println("Ctrl+D/Ctrl+Z to stop.");
	}
	
	private DefaultDependentTasksExecutor<Integer, Integer> newTaskExecutor(ExecutorService executorService) {
		DependentTasksExecutorConfig<Integer, Integer> config = new DependentTasksExecutorConfig<Integer, Integer>(
				new IgniteExecutionEngine<Integer, Integer>(executorService), new SleepyTaskProvider());
		return new DefaultDependentTasksExecutor<Integer, Integer>(config);
	}

	private void buildGraph(final DefaultDependentTasksExecutor<Integer, Integer> dexecutor) {
		dexecutor.addDependency(1, 2);
		dexecutor.addDependency(1, 2);
		dexecutor.addDependency(1, 3);
		dexecutor.addDependency(3, 4);
		dexecutor.addDependency(3, 5);
		dexecutor.addDependency(3, 6);
		dexecutor.addDependency(2, 7);
		dexecutor.addDependency(2, 9);
		dexecutor.addDependency(2, 8);
		dexecutor.addDependency(9, 10);
		dexecutor.addDependency(12, 13);
		dexecutor.addDependency(13, 4);
		dexecutor.addDependency(13, 14);
		dexecutor.addIndependent(11);
	}
	
	private static class SleepyTaskProvider implements TaskProvider<Integer, Integer> {

		public Task<Integer, Integer> provideTask(final Integer id) {

			return new Task<Integer, Integer>() {

				private static final long serialVersionUID = 1L;

				public Integer execute() {
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return id;
				}
			};
		}
	}

}
