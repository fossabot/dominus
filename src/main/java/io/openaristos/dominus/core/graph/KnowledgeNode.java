package io.openaristos.dominus.core.graph;

import io.openaristos.dominus.core.LocalMasterEntity;

public class KnowledgeNode {

  private final LocalMasterEntity masterEntity;

  private KnowledgeNode(final LocalMasterEntity masterEntity) {
    this.masterEntity = masterEntity;
  }

  public static KnowledgeNode of(LocalMasterEntity masterEntity) {
    return new KnowledgeNode(masterEntity);
  }

  @Override
  public int hashCode() {
    return getUid().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof KnowledgeNode)) {
      return false;
    }

    return ((KnowledgeNode) obj).getUid().equals(getUid());
  }

  @Override
  public String toString() {
    return getMasterEntity().toString();
  }

  public LocalMasterEntity getMasterEntity() {
    return masterEntity;
  }

  public String getUid() {
    return masterEntity.getUid();
  }
}
