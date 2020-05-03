package io.openaristos.dominus.core;

import com.google.common.collect.*;
import io.openaristos.dominus.Descriptor;
import io.openaristos.dominus.ResolveEntityRequest;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

@EqualsAndHashCode
public class ResolveQuery {
  @EqualsAndHashCode.Exclude private final Map<LocalEntityModel.Attribute, String> descriptors;

  @EqualsAndHashCode.Exclude
  private final Map<String, Pair<LocalEntityModel.Attribute, String>> descriptorsByFieldName;

  private final String uid;

  public ResolveQuery(Map<LocalEntityModel.Attribute, String> descriptors) {
    this.descriptors = descriptors;
    this.descriptorsByFieldName = Maps.newHashMap();

    StringBuilder canonical = new StringBuilder();

    descriptors.forEach(
        (x, y) -> {
          descriptorsByFieldName.put(x.getName(), Pair.of(x, descriptors.get(x)));

          canonical.append(x.getName());
          canonical.append(y);
        });

    uid = Integer.toString(canonical.toString().hashCode());
  }

  public static ResolveQuery of(EntityType entityType, ResolveEntityRequest request) {
    final Map<LocalEntityModel.Attribute, String> descriptors = Maps.newHashMap();

    for (Descriptor descriptor : request.getDescriptorsList()) {
      final LocalEntityModel.Attribute attribute =
          entityType.getLocalEntityModel().getAttributeByName().get(descriptor.getKey());
      descriptors.put(attribute, descriptor.getValue());
    }

    return new ResolveQuery(descriptors);
  }

  public static ResolveQuery of(Map<LocalEntityModel.Attribute, String> descriptors) {
    return new ResolveQuery(descriptors);
  }

  public String getUid() {
    return uid;
  }

  public Map<LocalEntityModel.Attribute, String> getDescriptors() {
    return descriptors;
  }

  public Map<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> getTemporalQueryAttributes() {
    final Map<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> results = Maps.newHashMap();

    descriptors.forEach(
        (attribute, value) -> {
          results.put(
              attribute,
              ImmutableMap.of(
                  value,
                  TreeRangeSet.create(
                      ImmutableSet.of(Range.open(Long.MIN_VALUE, Long.MAX_VALUE)))));
        });

    return results;
  }

  public Map<String, Pair<LocalEntityModel.Attribute, String>> getDescriptorsByFieldName() {
    return descriptorsByFieldName;
  }
}
