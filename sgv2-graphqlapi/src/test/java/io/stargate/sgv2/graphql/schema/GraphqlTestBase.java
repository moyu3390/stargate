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
package io.stargate.sgv2.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public abstract class GraphqlTestBase {

  private GraphQL graphql;
  private GraphQLSchema graphqlSchema;
  @Mock private StargateBridgeClient bridge;
  @Captor private ArgumentCaptor<Query> queryCaptor;

  protected abstract GraphQLSchema createGraphqlSchema();

  protected Map<String, CqlKeyspaceDescribe> getCqlSchema() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setupEnvironment() {
    Map<String, CqlKeyspaceDescribe> cqlSchema = getCqlSchema();
    when(bridge.getKeyspace(anyString()))
        .thenAnswer(
            i -> {
              String keyspaceName = i.getArgument(0);
              return Optional.ofNullable(cqlSchema.get(keyspaceName));
            });
    when(bridge.getTables(anyString()))
        .thenAnswer(
            i -> {
              String keyspaceName = i.getArgument(0);
              CqlKeyspaceDescribe describe = cqlSchema.get(keyspaceName);
              return describe == null ? Collections.emptyList() : describe.getTablesList();
            });
    when(bridge.getTable(anyString(), anyString()))
        .thenAnswer(
            i -> {
              String keyspaceName = i.getArgument(0);
              String tableName = i.getArgument(1);
              CqlKeyspaceDescribe describe = cqlSchema.get(keyspaceName);
              return Optional.ofNullable(describe)
                  .flatMap(
                      d ->
                          describe.getTablesList().stream()
                              .filter(t -> tableName.equals(t.getName()))
                              .findFirst());
            });

    when(bridge.executeQuery(queryCaptor.capture())).thenReturn(Response.newBuilder().build());

    graphqlSchema = createGraphqlSchema();
    graphql =
        GraphQL.newGraphQL(graphqlSchema)
            .mutationExecutionStrategy(new AsyncExecutionStrategy())
            .build();
  }

  private ExecutionResult executeGraphql(String query) {
    StargateGraphqlContext context = mock(StargateGraphqlContext.class);
    when(context.getBridge()).thenReturn(bridge);
    return graphql.execute(ExecutionInput.newExecutionInput(query).context(context).build());
  }

  /** Executes a GraphQL query and asserts that it generates the given CQL query. */
  protected void assertQuery(String graphqlQuery, String expectedCqlQuery) {
    ExecutionResult result = executeGraphql(graphqlQuery);
    assertThat(result.getErrors()).isEmpty();
    assertThat(getCapturedCql()).isEqualTo(expectedCqlQuery);
  }

  private String getCapturedCql() {
    Query query = queryCaptor.getValue();
    assertThat(query).isNotNull();
    return query.getCql();
  }

  /** Executes a GraphQL query and asserts that it returns the given JSON response. */
  protected void assertResponse(String graphqlQuery, String expectedJsonResponse) {
    ExecutionResult result = executeGraphql(graphqlQuery);
    assertThat(result.getErrors()).isEmpty();
    assertThat((Object) result.getData()).isEqualTo(parseJson(expectedJsonResponse));
  }

  /**
   * Executes a GraphQL query and asserts that it generates an error containing the given message.
   */
  protected void assertError(String graphqlQuery, String expectedError) {
    ExecutionResult result = executeGraphql(graphqlQuery);
    assertThat(result.getErrors()).as("Expected an error but the query succeeded").isNotEmpty();
    GraphQLError error = result.getErrors().get(0);
    assertThat(error.getMessage()).contains(expectedError);
  }

  private Map<?, ?> parseJson(String json) {
    try {
      return new ObjectMapper().readValue(json, Map.class);
    } catch (IOException e) {
      Assertions.fail("Unexpected error while parsing " + json, e);
      return null; // never reached
    }
  }
}
