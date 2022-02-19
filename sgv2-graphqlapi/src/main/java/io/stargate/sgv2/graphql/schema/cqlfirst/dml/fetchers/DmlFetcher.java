package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public abstract class DmlFetcher<ResultT> extends CassandraFetcher<ResultT> {

  protected final CqlTable table;
  protected final NameMapping nameMapping;
  protected final DbColumnGetter dbColumnGetter;

  protected DmlFetcher(CqlTable table, NameMapping nameMapping) {
    this.table = table;
    this.nameMapping = nameMapping;
    this.dbColumnGetter = new DbColumnGetter(nameMapping);
  }

  protected QueryParameters buildParameters(DataFetchingEnvironment environment) {
    Map<String, Object> options = environment.getArgument("options");
    if (options == null) {
      return DEFAULT_PARAMETERS;
    }

    QueryParameters.Builder builder = DEFAULT_PARAMETERS.toBuilder();

    Object consistency = options.get("consistency");
    if (consistency != null) {
      builder.setConsistency(
          ConsistencyValue.newBuilder().setValue(Consistency.valueOf((String) consistency)));
    }

    Object serialConsistency = options.get("serialConsistency");
    if (serialConsistency != null) {
      builder.setSerialConsistency(
          ConsistencyValue.newBuilder().setValue(Consistency.valueOf((String) serialConsistency)));
    }

    Object pageSize = options.get("pageSize");
    if (pageSize != null) {
      builder.setPageSize(Int32Value.of((Integer) pageSize));
    }

    Object pageState = options.get("pageState");
    if (pageState != null) {
      builder.setPagingState(
          BytesValue.of(ByteString.copyFrom(Base64.getDecoder().decode((String) pageState))));
    }

    return builder.build();
  }

  protected List<BuiltCondition> buildConditions(
      CqlTable table, Map<String, Map<String, Object>> columnList) {
    if (columnList == null) {
      return ImmutableList.of();
    }
    List<BuiltCondition> where = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
      ColumnSpec column = dbColumnGetter.getColumn(table, clauseEntry.getKey());
      for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
        FilterOperator operator = FilterOperator.fromFieldName(condition.getKey());
        where.add(operator.buildCondition(column, condition.getValue(), nameMapping));
      }
    }
    return where;
  }

  protected List<BuiltCondition> buildClause(CqlTable table, DataFetchingEnvironment environment) {
    if (environment.containsArgument("filter")) {
      Map<String, Map<String, Object>> columnList = environment.getArgument("filter");
      return buildConditions(table, columnList);
    } else {
      Map<String, Object> value = environment.getArgument("value");
      if (value == null) {
        return ImmutableList.of();
      }
      List<BuiltCondition> relations = new ArrayList<>();
      for (Map.Entry<String, Object> entry : value.entrySet()) {
        ColumnSpec column = dbColumnGetter.getColumn(table, entry.getKey());
        Value whereValue = toDBValue(column.getType(), entry.getValue());
        relations.add(BuiltCondition.of(column.getName(), Predicate.EQ, whereValue));
      }
      return relations;
    }
  }

  private Value toDBValue(QueryOuterClass.TypeSpec type, Object value) {
    return DataTypeMapping.toDBValue(type, value, nameMapping);
  }
}
