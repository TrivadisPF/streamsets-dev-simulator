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

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.format.DataFormatType;

public class TimestampModeChooserValues extends BaseEnumChooserValues<DataFormatType> {
    public TimestampModeChooserValues() {
        super(TimestampModeType.class);
    }
}

