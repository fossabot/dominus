package io.openaristos.dominus.core.subscribers;

import com.google.common.collect.Lists;
import io.openaristos.dominus.core.subscribers.internal.PubSubExternalSubscriber;

import java.util.List;
import java.util.Properties;

public class SubscribersFactory {
  public static List<ExternalSubscriber> createFromProperties(final Properties properties) {
    final List<ExternalSubscriber> results = Lists.newArrayList();

    final String pubsub = properties.getProperty("subscribers.pubsub.enabled");
    if (pubsub != null && pubsub.equalsIgnoreCase("true")) {
      results.add(new PubSubExternalSubscriber(properties));
    }

    return results;
  }
}
