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

import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.common.grpc.KeyspaceInvalidationListener;
import io.stargate.sgv2.common.grpc.StargateBridgeSchema;
import io.stargate.sgv2.graphql.schema.CassandraFetcherExceptionHandler;
import io.stargate.sgv2.graphql.schema.cqlfirst.SchemaFactory;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages the {@link GraphQL} instances used by our REST resources.
 *
 * <p>This includes staying up to date with CQL schema changes.
 */
public class GraphqlCache implements KeyspaceInvalidationListener {

  private final StargateBridgeSchema schema;
  private final GraphQL ddlGraphql;
  private final ConcurrentMap<String, CompletionStage<Optional<GraphQL>>> dmlGraphqls =
      new ConcurrentHashMap<>();

  public GraphqlCache(StargateBridgeSchema schema) {
    this.schema = schema;
    this.ddlGraphql = newGraphql(SchemaFactory.newDdlSchema());
  }

  public GraphQL getDdl() {
    return ddlGraphql;
  }

  public CompletionStage<Optional<GraphQL>> getDml(String keyspaceName) {
    CompletionStage<Optional<GraphQL>> existing = dmlGraphqls.get(keyspaceName);
    if (existing != null) {
      return existing;
    }
    CompletableFuture<Optional<GraphQL>> mine = new CompletableFuture<>();
    existing = dmlGraphqls.putIfAbsent(keyspaceName, mine);
    if (existing != null) {
      return existing;
    }
    loadAsync(keyspaceName, mine);
    return mine;
  }

  @Override
  public void onKeyspaceInvalidated(String keyspaceName) {
    dmlGraphqls.remove(keyspaceName);
  }

  private void loadAsync(String keyspaceName, CompletableFuture<Optional<GraphQL>> toComplete) {
    schema
        .getKeyspaceAsync(keyspaceName)
        .thenAccept(cqlSchema -> toComplete.complete(buildDml(cqlSchema)))
        .exceptionally(
            throwable -> {
              // Surface to the caller, but don't leave a failed entry in the cache
              toComplete.completeExceptionally(throwable);
              dmlGraphqls.remove(keyspaceName);
              return null;
            });
  }

  private Optional<GraphQL> buildDml(CqlKeyspaceDescribe cqlSchema) {
    return Optional.ofNullable(cqlSchema).map(s -> newGraphql(SchemaFactory.newDmlSchema(s)));
  }

  private static GraphQL newGraphql(GraphQLSchema schema) {
    return GraphQL.newGraphQL(schema)
        .defaultDataFetcherExceptionHandler(CassandraFetcherExceptionHandler.INSTANCE)
        // Use parallel execution strategy for mutations (serial is default)
        .mutationExecutionStrategy(
            new AsyncExecutionStrategy(CassandraFetcherExceptionHandler.INSTANCE))
        .build();
  }
}
