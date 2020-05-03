package io.openaristos.dominus.core;

import lombok.EqualsAndHashCode;

// Represents a noun or thing, these are usually the type of things we want to know about, such as Country
@EqualsAndHashCode
public class EntityType {
  private final String name;

  @EqualsAndHashCode.Exclude private final LocalEntityModel localEntityModel;

  public EntityType(String name, LocalEntityModel localEntityModel) {
    this.name = name;
    this.localEntityModel = localEntityModel;
  }

  public static EntityType of(String name, LocalEntityModel localEntityModel) {
    return new EntityType(name, localEntityModel);
  }

  public String getName() {
    return name;
  }

  public LocalEntityModel getLocalEntityModel() {
    return localEntityModel;
  }
}
