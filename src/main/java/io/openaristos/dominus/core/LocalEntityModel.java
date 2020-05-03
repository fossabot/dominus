package io.openaristos.dominus.core;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.openaristos.dominus.EntityModel;
import lombok.EqualsAndHashCode;

import java.util.*;
import java.util.function.Predicate;

// An entity model helps disambiguate manifestations of real world entities
@EqualsAndHashCode
public class LocalEntityModel {
  public static Matcher EQUALITY_MATCHER =
      Matcher.of(
          "equality",
          matchingContext -> matchingContext.getSource().equalsIgnoreCase(matchingContext.target));
  private final Set<Attribute> attributes;
  private final TreeSet<Resolver> resolvers;
  private final Map<String, Attribute> attributeByName;

  public LocalEntityModel(Set<Attribute> attributes, Set<Resolver> resolvers) {
    this.attributes = attributes;

    this.resolvers = Sets.newTreeSet(Comparator.comparingInt(Resolver::getWeight).reversed());
    this.resolvers.addAll(resolvers);
    this.attributeByName = Maps.newHashMap();

    for (Attribute a : attributes) {
      attributeByName.put(a.getName(), a);
    }
  }

  public static LocalEntityModel of(EntityModel request) {
    return LocalEntityModel.of(
        mapAttributes(request.getAttributesList()), mapResolvers(request.getResolversList()));
  }

  public static LocalEntityModel of(Set<Attribute> attributes, Set<Resolver> resolvers) {
    return new LocalEntityModel(attributes, resolvers);
  }

  private static Set<Attribute> mapAttributes(
      List<io.openaristos.dominus.Attribute> attributesList) {
    final Set<Attribute> attributes = Sets.newHashSet();

    attributesList.forEach(
        x -> attributes.add(Attribute.of(x.getName(), AttributeType.STRING, EQUALITY_MATCHER)));

    return attributes;
  }

  private static Set<Resolver> mapResolvers(List<io.openaristos.dominus.Resolver> resolversList) {
    final Set<Resolver> resolvers = Sets.newHashSet();

    resolversList.forEach(
        x ->
            resolvers.add(
                Resolver.of(x.getName(), mapAttributes(x.getAttributesList()), x.getWeight())));

    return resolvers;
  }

  public Set<Attribute> getAttributes() {
    return attributes;
  }

  public TreeSet<Resolver> getResolvers() {
    return resolvers;
  }

  public Map<String, Attribute> getAttributeByName() {
    return attributeByName;
  }

  // Attributes are assigned a single data type
  public enum AttributeType {
    STRING,
    NUMERIC,
    DATE,
    BOOLEAN
  }

  // Attributes are used to characterize an entity with small pieces of data
  @EqualsAndHashCode
  public static class Attribute {
    private final String name;
    private final AttributeType type;

    @EqualsAndHashCode.Exclude private final Map<String, String> context;

    @EqualsAndHashCode.Exclude private final Matcher matcher;

    public Attribute(
        String name, AttributeType type, Map<String, String> context, Matcher matcher) {
      this.name = name;
      this.type = type;
      this.context = context;
      this.matcher = matcher;
    }

    public static Attribute of(
        String name, AttributeType type, Map<String, String> context, Matcher matcher) {
      return new Attribute(name, type, context, matcher);
    }

    public static Attribute of(String name, AttributeType type, Matcher matcher) {
      return new Attribute(name, type, ImmutableMap.of(), matcher);
    }

    public String getName() {
      return name;
    }

    public AttributeType getType() {
      return type;
    }

    public Map<String, String> getContext() {
      return context;
    }

    public Matcher getMatcher() {
      return matcher;
    }
  }

  // Resolvers combine one or more attributes together in order to uniquely identify an entity
  @EqualsAndHashCode
  public static class Resolver {
    private final String name;
    private final int weight;

    @EqualsAndHashCode.Exclude private final Set<Attribute> attributes;

    @EqualsAndHashCode.Exclude private final Map<String, Attribute> attributeNameToAttribute;

    public Resolver(String name, Set<Attribute> attributes, int weight) {
      this.name = name;
      this.attributes = attributes;
      this.weight = weight;

      this.attributeNameToAttribute = Maps.newHashMap();
      for (Attribute attribute : attributes) {
        attributeNameToAttribute.put(attribute.getName(), attribute);
      }
    }

    public static Resolver of(String name, Set<Attribute> attributes, int weight) {
      return new Resolver(name, attributes, weight);
    }

    public String getName() {
      return name;
    }

    public Set<Attribute> getAttributes() {
      return attributes;
    }

    public int getWeight() {
      return weight;
    }

    public Map<String, Attribute> getAttributeNameToAttribute() {
      return attributeNameToAttribute;
    }

    public boolean inferiorTo(Resolver other) {
      return other.weight > weight;
    }
  }

  @EqualsAndHashCode
  public static class MatchingContext {
    private final String source;

    private final String target;
    private final Attribute attribute;

    public MatchingContext(String source, String target, Attribute attribute) {
      this.source = source;
      this.target = target;
      this.attribute = attribute;
    }

    public String getSource() {
      return source;
    }

    public String getTarget() {
      return target;
    }

    public Attribute getAttribute() {
      return attribute;
    }
  }

  // The matchers are predicates, they compare attributes, and decide whether or not they are the same.
  @EqualsAndHashCode
  public static class Matcher {

    private final String name;
    private final Predicate<MatchingContext> predicate;

    public Matcher(String name, Predicate<MatchingContext> predicate) {
      this.name = name;
      this.predicate = predicate;
    }

    public static Matcher of(String name, Predicate<MatchingContext> predicate) {
      return new Matcher(name, predicate);
    }

    public String getName() {
      return name;
    }

    public Predicate<MatchingContext> getPredicate() {
      return predicate;
    }
  }
}
