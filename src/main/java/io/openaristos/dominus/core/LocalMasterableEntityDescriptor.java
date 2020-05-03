package io.openaristos.dominus.core;

import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;

@EqualsAndHashCode
public class LocalMasterableEntityDescriptor {

  private LocalEntityModel.Attribute key;
  private String value;

  public LocalMasterableEntityDescriptor() {}

  private LocalMasterableEntityDescriptor(LocalEntityModel.Attribute key, String value) {
    this.key = key;
    this.value = value;
  }

  public static LocalMasterableEntityDescriptor of(LocalEntityModel.Attribute key, String value) {
    return new LocalMasterableEntityDescriptor(key, value);
  }

  public LocalEntityModel.Attribute getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public Pair<LocalEntityModel.Attribute, String> getKeyValue() {
    return Pair.of(key, value);
  }
}
