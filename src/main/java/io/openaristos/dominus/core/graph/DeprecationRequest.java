package io.openaristos.dominus.core.graph;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class DeprecationRequest {
  private final KnowledgeNode old;
  private final KnowledgeNode current;
  private final long sequenceId;

  public DeprecationRequest(KnowledgeNode old, KnowledgeNode current, long sequenceId) {
    this.old = old;
    this.current = current;
    this.sequenceId = sequenceId;
  }

  public KnowledgeNode getOld() {
    return old;
  }

  public KnowledgeNode getCurrent() {
    return current;
  }

  public long getSequenceId() {
    return sequenceId;
  }
}
