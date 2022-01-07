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
package io.stargate.sgv2.restsvc.driver;

import com.datastax.oss.driver.api.core.CqlSession;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import org.glassfish.hk2.api.Factory;

/**
 * Provides the stub that we created in {@link CreateScopedCqlSessionFilter} to the Jersey context,
 * so that it can be injected into resource methods with {@code @Context}.
 */
public class ScopedCqlSessionFactory implements Factory<CqlSession> {

  private final ContainerRequestContext context;

  @Inject
  public ScopedCqlSessionFactory(ContainerRequestContext context) {
    this.context = context;
  }

  @Override
  public CqlSession provide() {
    return (CqlSession) context.getProperty(CreateScopedCqlSessionFilter.SCOPED_SESSION_KEY);
  }

  @Override
  public void dispose(CqlSession instance) {
    // intentionally empty
  }
}