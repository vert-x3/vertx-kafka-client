package io.vertx.kafka.admin;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class FeatureMetadata {
  private List<FinalizedFeature> finalizedFeaturesData;
  private List<SupportedFeature> supportedFeaturesData;
  private Long finalizedFeaturesEpoch;
}
