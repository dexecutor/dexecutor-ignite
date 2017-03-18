package com.github.dexecutor.ignite;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CollectionConfiguration;

import com.github.dexecutor.core.DexecutorState;
import com.github.dexecutor.core.Phase;
import com.github.dexecutor.core.graph.Dag;
import com.github.dexecutor.core.graph.DefaultDag;
import com.github.dexecutor.core.graph.Node;
import com.github.dexecutor.core.graph.Traversar;
import com.github.dexecutor.core.graph.TraversarAction;
import com.github.dexecutor.core.graph.Validator;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.ExecutionResults;
/**
 *
 * @author Nadeem Mohammad
 *
 * @param <T> Type of Node/Task ID
 * @param <R> Type of Node/Task result
 */
public class IgniteDexecutorState<T extends Comparable<T>, R> implements DexecutorState<T, R> {

	private final String CACHE_ID_PHASE;
	private final String CACHE_ID_GRAPH;
	private final String CACHE_ID_NODES_COUNT;
	private final String CACHE_ID_PROCESSED_NODES;
	private final String CACHE_ID_DISCONDINUED_NODES;
	private final String CACHE_ID_ERRORED_NODES;
	
	private IgniteCache<String, Object> distributedCache;

	private IgniteAtomicLong nodesCount;
	private Collection<Node<T, R>> processedNodes;
	private Collection<Node<T, R>> discontinuedNodes;
	private Collection<ExecutionResult<T, R>> erroredNodes;
	
	public IgniteDexecutorState(final String cacheName, final Ignite ignite) {
		CACHE_ID_PHASE = cacheName + "-phase";
		CACHE_ID_GRAPH = cacheName + "-graph";
		CACHE_ID_NODES_COUNT = cacheName + "-nodes-count";
		CACHE_ID_PROCESSED_NODES = cacheName + "-processed-nodes";
		CACHE_ID_DISCONDINUED_NODES = cacheName + "-discontinued-nodes";
		CACHE_ID_ERRORED_NODES = cacheName + "-errored-nodes";
		
		this.distributedCache = ignite.getOrCreateCache(cacheName + "distribute");
		this.distributedCache.put(CACHE_ID_PHASE, Phase.BUILDING);
		
		this.distributedCache.put(CACHE_ID_GRAPH, new DefaultDag<>());
		
		
		
		this.nodesCount = ignite.atomicLong(
				CACHE_ID_NODES_COUNT, // Atomic long name.
			    0,            // Initial value.
			    true         // Create if it does not exist.
			);
		
		CollectionConfiguration setCfg = new CollectionConfiguration();

        setCfg.setAtomicityMode(TRANSACTIONAL);
        setCfg.setCacheMode(PARTITIONED);
		this.processedNodes = ignite.set(
				CACHE_ID_PROCESSED_NODES, // Queue name.
				setCfg       // Collection configuration.
			);
		
		
		this.discontinuedNodes = ignite.set(
				CACHE_ID_DISCONDINUED_NODES, // Queue name.
				setCfg       // Collection configuration.
			);
		
		this.erroredNodes = ignite.set(
				CACHE_ID_ERRORED_NODES, // Queue name.
				setCfg       // Collection configuration.
			);

	}
	
	@SuppressWarnings("unchecked")
	private Dag<T, R> getDag() {
		return (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
	}

	@Override
	public void addIndependent(T nodeValue) {
		Dag<T, R> graph = this.getDag();
		graph.addIndependent(nodeValue);
		this.distributedCache.put(CACHE_ID_GRAPH, graph);
	}

	@Override
	public void addDependency(T evalFirstValue, T evalAfterValue) {
		Dag<T, R> graph = this.getDag();
		graph.addDependency(evalFirstValue, evalAfterValue);	
		this.distributedCache.put(CACHE_ID_GRAPH, graph);
	}

	@Override
	public void addAsDependentOnAllLeafNodes(T nodeValue) {
		Dag<T, R> dag = this.getDag();
		dag.addAsDependentOnAllLeafNodes(nodeValue);
		this.distributedCache.put(CACHE_ID_GRAPH, dag);
		
	}

	@Override
	public void addAsDependencyToAllInitialNodes(T nodeValue) {
		Dag<T, R> dag = this.getDag();
		dag.addAsDependencyToAllInitialNodes(nodeValue);
		this.distributedCache.put(CACHE_ID_GRAPH, dag);
	}

	@Override
	public Set<Node<T, R>> getInitialNodes() {
		return this.getDag().getInitialNodes();
	}

	@Override
	public Node<T, R> getGraphNode(T id) {
		return this.getDag().get(id);
	}

	@Override
	public int graphSize() {
		return this.getDag().size();
	}

	@Override
	public Set<Node<T, R>> getNonProcessedRootNodes() {
		return this.getDag().getNonProcessedRootNodes();
	}

	@Override
	public void validate(Validator<T, R> validator) {
		validator.validate(this.getDag());		
	}

	@Override
	public void setCurrentPhase(Phase currentPhase) {
		this.distributedCache.put(CACHE_ID_PHASE, currentPhase);
	}

	@Override
	public Phase getCurrentPhase() {
		return (Phase) this.distributedCache.get(CACHE_ID_PHASE);
	}

	@Override
	public int getUnProcessedNodesCount() {
		return (int) this.nodesCount.get();
	}

	@Override
	public void incrementUnProcessedNodesCount() {
		this.nodesCount.incrementAndGet();		
	}

	@Override
	public void decrementUnProcessedNodesCount() {
		this.nodesCount.decrementAndGet();		
	}

	@Override
	public boolean shouldProcess(Node<T, R> node) {
		return !isAlreadyProcessed(node) && allIncomingNodesProcessed(node);
	}
	
	private boolean isAlreadyProcessed(final Node<T, R> node) {
		return this.processedNodes.contains(node);
	}

	private boolean allIncomingNodesProcessed(final Node<T, R> node) {
		if (node.getInComingNodes().isEmpty() || areAlreadyProcessed(node.getInComingNodes())) {
			return true;
		}
		return false;
	}

	private boolean areAlreadyProcessed(final Set<Node<T, R>> nodes) {
        return this.processedNodes.containsAll(nodes);
    }

	@Override
	public void markProcessingDone(Node<T, R> node) {
		this.processedNodes.add(node);		
	}

	@Override
	public Collection<Node<T, R>> getProcessedNodes() {
		return new ArrayList<>(this.processedNodes);
	}

	@Override
	public boolean isDiscontinuedNodesNotEmpty() {
		return !this.discontinuedNodes.isEmpty();
	}

	@Override
	public Collection<Node<T, R>> getDiscontinuedNodes() {
		return new ArrayList<Node<T, R>>(this.discontinuedNodes);
	}

	@Override
	public void markDiscontinuedNodesProcessed() {
		this.discontinuedNodes.clear();		
	}

	@Override
	public void processAfterNoError(Collection<Node<T, R>> nodes) {
		this.discontinuedNodes.addAll(nodes);		
	}

	@Override
	public void print(Traversar<T, R> traversar, TraversarAction<T, R> action) {
		traversar.traverse(this.getDag(), action);		
	}

	@Override
	public void addErrored(ExecutionResult<T, R> task) {
		this.erroredNodes.add(task);		
	}

	@Override
	public void removeErrored(ExecutionResult<T, R> task) {
		this.erroredNodes.remove(task);		
	}

	@Override
	public ExecutionResults<T, R> getErrored() {
		ExecutionResults<T, R> result = new ExecutionResults<>();
		for (ExecutionResult<T, R> r : this.erroredNodes) {
			result.add(r);
		}
		return result;
	}

	@Override
	public int erroredCount() {
		return this.erroredNodes.size();
	}

	@Override
	public void forcedStop() {
		// TODO Auto-generated method stub
		
	}
}
