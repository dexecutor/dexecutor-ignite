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

/**
 * 
 * Terminal #1
 * mvn test-compile exec:java -Djava.net.preferIPv4Stack=true -Dexec.mainClass="com.github.dexecutor.ignite.Node" -Dexec.classpathScope="test" -Dexec.args="s node-A"
 * 
 * Terminal #2
 * mvn test-compile exec:java -Djava.net.preferIPv4Stack=true -Dexec.mainClass="com.github.dexecutor.ignite.Node" -Dexec.classpathScope="test" -Dexec.args="s node-B"
 * 
 * Terminal #3
 * mvn test-compile exec:java  -Dexec.classpathScope="test" -Djava.net.preferIPv4Stack=true -Dexec.mainClass="com.github.dexecutor.ignite.Node" -Dexec.args="m node-C"
 * 
 * @author Nadeem Mohammad
 *
 */
public class Node {

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			new Job().run(true, "TEST_NODE");
		} else {			
			new Job().run(isMaster(args[0]), args[1]);
		}
	}

	private static boolean isMaster(String string) {
		return string.equalsIgnoreCase("m");
	}
}
