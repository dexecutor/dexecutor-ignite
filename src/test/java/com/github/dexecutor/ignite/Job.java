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

import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.Duration;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;

public class Job {

	public void run(boolean isMaster, final String nodeName) throws Exception {

		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setIgniteInstanceName(nodeName); 

		Ignite ignite = Ignition.start(cfg); 

		if (isMaster) {
			DefaultDexecutor<Integer, Integer> dexecutor = newTaskExecutor(ignite);

			buildGraph(dexecutor);
			dexecutor.execute(new ExecutionConfig().scheduledRetrying(4, new Duration(1, TimeUnit.SECONDS)));
		}

		System.out.println("Ctrl+D/Ctrl+Z to stop.");
	}
	
	private DefaultDexecutor<Integer, Integer> newTaskExecutor(final Ignite ignite) {
		IgniteDexecutorState<Integer, Integer> dexecutorState = new IgniteDexecutorState<Integer, Integer>("test", ignite);
		DexecutorConfig<Integer, Integer> config = new DexecutorConfig<Integer, Integer>(
				new IgniteExecutionEngine<Integer, Integer>(dexecutorState, ignite.compute()), new SleepyTaskProvider(ignite));
		config.setDexecutorState(dexecutorState);
		return new DefaultDexecutor<Integer, Integer>(config);
	}

	private void buildGraph(final DefaultDexecutor<Integer, Integer> dexecutor) {
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
		final IgniteAtomicLong count;

		public SleepyTaskProvider(final Ignite ignite) {
			count = ignite.atomicLong("count", 0, true);
		}

		public Task<Integer, Integer> provideTask(final Integer id) {

			return new Task<Integer, Integer>() {

				private static final long serialVersionUID = 1L;

				public Integer execute() {
					try {
						/*if (id == 2) {
							count.incrementAndGet();
							System.out.println("Count is " + count.get());
							if (count.get() < 3) {							
								throw new IllegalArgumentException("Invalid task");
							}						
						}*/
						System.out.println("Executing :*****  " +  getId());
						Thread.sleep(time(0, 5000));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return id;
				}

				private long time(int min, int max) {
					return min + (int)(Math.random() * ((max - min) + 1));
				}
			};
		}
	}
}
