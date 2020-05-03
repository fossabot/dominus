package io.openaristos.dominus.core;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.openaristos.dominus.Descriptor;
import io.openaristos.dominus.MasterableEntityIdentity;
import lombok.EqualsAndHashCode;

import java.util.Map;
import java.util.Set;

@EqualsAndHashCode
public class LocalMasterableEntityIdentity {
  private final EntityType entityType;
  private final String source;
  private final Set<LocalMasterableEntityDescriptor> masterableEntityDescriptors;
  @EqualsAndHashCode.Exclude private final Map<LocalEntityModel.Attribute, String> descriptorMap;

  private final String uid;

  private LocalMasterableEntityIdentity(
      String uid,
      EntityType entityType,
      String perspective,
      Set<LocalMasterableEntityDescriptor> masterableEntityDescriptors) {
    this.uid = uid;
    this.entityType = entityType;
    this.source = perspective;
    this.masterableEntityDescriptors = masterableEntityDescriptors;

    this.descriptorMap = Maps.newHashMap();

    if (!masterableEntityDescriptors.isEmpty()) {
      masterableEntityDescriptors.forEach(
          x -> {
            if (!entityType
                .getLocalEntityModel()
                .getAttributeByName()
                .containsKey(x.getKey().getName())) {
              throw new RuntimeException("invalid key");
            }
            descriptorMap.put(
                entityType.getLocalEntityModel().getAttributeByName().get(x.getKey().getName()),
                x.getValue());
          });
    }
  }

  public static LocalMasterableEntityIdentity of(
      String uid,
      EntityType entityType,
      String source,
      LocalMasterableEntityDescriptor masterableEntityDescriptor) {
    return new LocalMasterableEntityIdentity(
        uid, entityType, source, ImmutableSet.of(masterableEntityDescriptor));
  }

  public static LocalMasterableEntityIdentity of(
      String uid,
      EntityType entityType,
      String source,
      Set<LocalMasterableEntityDescriptor> masterableEntityDescriptors) {
    return new LocalMasterableEntityIdentity(uid, entityType, source, masterableEntityDescriptors);
  }

  public static LocalMasterableEntityIdentity of(
      String uid, EntityType entityType, String perspective, MasterableEntityIdentity identity) {
    final Set<LocalMasterableEntityDescriptor> descriptors = Sets.newHashSet();

    for (Descriptor d : identity.getDescriptorsList()) {
      descriptors.add(
          LocalMasterableEntityDescriptor.of(
              entityType.getLocalEntityModel().getAttributeByName().get(d.getKey()), d.getValue()));
    }

    return new LocalMasterableEntityIdentity(uid, entityType, perspective, descriptors);
  }

  public String getSource() {
    return source;
  }

  public Set<LocalMasterableEntityDescriptor> getMasterableEntityDescriptors() {
    return masterableEntityDescriptors;
  }

  public Map<LocalEntityModel.Attribute, String> getDescriptorMap() {
    return descriptorMap;
  }

  public EntityType getEntityType() {
    return entityType;
  }

  public String getUid() {
    return uid;
  }
}
