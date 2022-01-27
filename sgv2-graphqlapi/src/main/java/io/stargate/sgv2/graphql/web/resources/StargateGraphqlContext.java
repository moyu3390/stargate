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
package io.stargate.sgv2.graphql.web.resources;

import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import javax.servlet.http.HttpServletRequest;

public class StargateGraphqlContext {

  private final HttpServletRequest httpRequest;
  private final StargateBridgeClient bridge;
  private final GraphqlCache graphqlCache;

  private volatile boolean overloaded;

  public StargateGraphqlContext(
      HttpServletRequest httpRequest, StargateBridgeClient bridge, GraphqlCache graphqlCache) {
    this.httpRequest = httpRequest;
    this.bridge = bridge;
    this.graphqlCache = graphqlCache;
  }

  public StargateBridgeClient getBridge() {
    return bridge;
  }

  /**
   * Records the fact that at least one CQL query in the current execution failed with an OVERLOADED
   * error. This will be translated into an HTTP 429 error at the resource layer.
   */
  public void setOverloaded() {
    this.overloaded = true;
  }

  public boolean isOverloaded() {
    return overloaded;
  }
}
