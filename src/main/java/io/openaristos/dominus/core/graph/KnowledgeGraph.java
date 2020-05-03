package io.openaristos.dominus.core.graph;

import io.openaristos.dominus.TraversalRequest;
import io.openaristos.dominus.TraversalResponse;

public interface KnowledgeGraph {
  boolean append(KnowledgeNode k);

  boolean relate(KnowledgeNode s, KnowledgeNode t, KnowledgeEdge e);

  void deprecate(KnowledgeNode s, KnowledgeNode t, long sequenceId);

  void lock();

  void unlock();

  void flush();

  TraversalResponse resolveTraversal(TraversalRequest request);
}
