package io.openaristos.dominus.core.subscribers;

import io.openaristos.dominus.core.EntityType;
import io.openaristos.dominus.core.EntityUniverse;
import io.openaristos.dominus.core.graph.KnowledgeGraph;

import java.util.Map;

public interface ExternalSubscriber {
  void start();

  void stop();

  void setup(
      final Map<String, EntityType> entityTypeMap,
      final Map<String, EntityUniverse> universeMap,
      final KnowledgeGraph knowledgeGraph);
}
