/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time;

import com.streamsets.pipeline.api.Label;

public enum TimestampModeType implements Label {
  ABSOLUTE("Absolute"),
  ABSOLUTE_WITH_START("Absolute with Start Timestamp"),
  RELATIVE_FROM_ANCHOR("Relative from Anchor Timestamp"),
  RELATIVE_FROM_PREVIOUS("Relative from Previous Event"),
  FIXED("Fixed (not yet supported)"),
  ;

  private String label;

  TimestampModeType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
