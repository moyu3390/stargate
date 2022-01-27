package io.stargate.sgv2.graphql.schema;

import com.google.protobuf.Int32Value;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;

/** Base class for fetchers that access the Cassandra backend. */
public abstract class CassandraFetcher<ResultT> implements DataFetcher<ResultT> {

  public static final ConsistencyValue DEFAULT_CONSISTENCY =
      ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_QUORUM).build();
  public static final ConsistencyValue DEFAULT_SERIAL_CONSISTENCY =
      ConsistencyValue.newBuilder().setValue(Consistency.SERIAL).build();
  public static final int DEFAULT_PAGE_SIZE = 100;

  public static final QueryParameters DEFAULT_PARAMETERS =
      QueryParameters.newBuilder()
          .setPageSize(Int32Value.of(DEFAULT_PAGE_SIZE))
          .setConsistency(DEFAULT_CONSISTENCY)
          .setSerialConsistency(DEFAULT_SERIAL_CONSISTENCY)
          .build();

  @Override
  public final ResultT get(DataFetchingEnvironment environment) throws Exception {

    // Small convenience: subclasses could just call environment.getContext() directly, but they'd
    // have to cast every time
    StargateGraphqlContext context = environment.getContext();

    return get(environment, context);
  }

  protected abstract ResultT get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) throws Exception;
}
