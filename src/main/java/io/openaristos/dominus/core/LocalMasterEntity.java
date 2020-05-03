package io.openaristos.dominus.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import lombok.EqualsAndHashCode;

import java.util.*;
import java.util.stream.Collectors;

@EqualsAndHashCode
public class LocalMasterEntity {

  private final String uid;
  private final String entityTypeCode;

  @EqualsAndHashCode.Exclude
  private final Map<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> descriptors;

  @EqualsAndHashCode.Exclude
  private final Map<LocalMasterableEntityIdentity, Set<LocalMasterEntityMembership>> identities;

  public LocalMasterEntity(
      String entityTypeCode,
      Map<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> descriptors) {
    this.entityTypeCode = entityTypeCode;

    final Set<String> canonicalComponents = Sets.newTreeSet(String::compareTo);

    descriptors.forEach(
        (x, y) -> {
          canonicalComponents.add(x.getName());
          canonicalComponents.addAll(y.keySet());
        });

    this.uid = UUID.randomUUID().toString();

    this.descriptors = descriptors;
    this.identities = Maps.newHashMap();
  }

  public String getUid() {
    return uid;
  }

  public Map<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> getDescriptors() {
    return descriptors;
  }

  public Map<LocalMasterableEntityIdentity, Set<LocalMasterEntityMembership>> getMemberIdentities() {
    return identities;
  }

  @Override
  public String toString() {
    final List<String> parts = Lists.newArrayList();

    descriptors.forEach(
        (x, y) -> parts.add(String.format("%s = [%s]", x.getName(), String.join(",", y.keySet()))));

    return String.join(",", parts);
  }

  public int state() {
    while (true) {
      try {
      return this.getDescriptors().keySet().stream()
          .map(LocalEntityModel.Attribute::getName)
          .collect(Collectors.toSet())
          .hashCode();
      } catch(ConcurrentModificationException ignored) {
      }
    }
  }

  public String getEntityTypeCode() {
    return entityTypeCode;
  }

}
