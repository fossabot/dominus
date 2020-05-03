package io.openaristos.dominus.core.graph;

import com.google.common.collect.RangeSet;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@EqualsAndHashCode
public class KnowledgeEdge {
  private String uid;
  private String relationship;

  @EqualsAndHashCode.Exclude private RangeSet<Long> validDts;

  private KnowledgeEdge(String uid, String relationship, RangeSet<Long> validDts) {
    this.uid = uid;
    this.relationship = relationship;
    this.validDts = validDts;
  }

  public static KnowledgeEdge of(String relationship, RangeSet<Long> validDts) {
    return new KnowledgeEdge(UUID.randomUUID().toString(), relationship, validDts);
  }

  public RangeSet<Long> getValidDts() {
    return validDts;
  }

  public String getRelationship() {
    return relationship;
  }

  @Override
  public String toString() {
    return relationship;
  }
}
