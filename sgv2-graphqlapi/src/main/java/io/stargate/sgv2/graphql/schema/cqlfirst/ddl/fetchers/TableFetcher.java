package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;

public abstract class TableFetcher extends DdlQueryFetcher {

  @Override
  protected String buildCql(DataFetchingEnvironment environment, StargateGraphqlContext context) {
    String keyspaceName = environment.getArgument("keyspaceName");
    String tableName = environment.getArgument("tableName");
    return buildCql(environment, keyspaceName, tableName);
  }

  protected abstract String buildCql(
      DataFetchingEnvironment environment, String keyspaceName, String tableName);
}
