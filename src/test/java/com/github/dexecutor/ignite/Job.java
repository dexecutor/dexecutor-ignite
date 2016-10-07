package com.github.dexecutor.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
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
			DefaultDependentTasksExecutor<Integer, Integer> dexecutor = newTaskExecutor(ignite.compute().withAsync());

			buildGraph(dexecutor);
			dexecutor.execute(ExecutionConfig.TERMINATING);
		}

		System.out.println("Ctrl+D/Ctrl+Z to stop.");
	}
	
	private DefaultDependentTasksExecutor<Integer, Integer> newTaskExecutor(IgniteCompute igniteCompute) {
		DependentTasksExecutorConfig<Integer, Integer> config = new DependentTasksExecutorConfig<Integer, Integer>(
				new IgniteExecutionEngine<Integer, Integer>(igniteCompute), new SleepyTaskProvider());
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
						Thread.sleep(time(0, 5000));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return id;
				}

				private long time(int Min, int Max) {
					return Min + (int)(Math.random() * ((Max - Min) + 1));
				}
			};
		}
	}

}
