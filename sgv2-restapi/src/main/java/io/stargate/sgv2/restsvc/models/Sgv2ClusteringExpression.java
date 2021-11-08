/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

public class Sgv2ClusteringExpression {

  private final String column;
  private final String order;

  @JsonCreator
  public Sgv2ClusteringExpression(
      @JsonProperty("name") String column, @JsonProperty("order") String order) {
    this.column = column;
    this.order = order;
  }

  @ApiModelProperty(required = true, value = "The name of the column to order by")
  public String getColumn() {
    return column;
  }

  @ApiModelProperty(required = true, value = "The clustering order", allowableValues = "asc,desc")
  public String getOrder() {
    return order;
  }
}