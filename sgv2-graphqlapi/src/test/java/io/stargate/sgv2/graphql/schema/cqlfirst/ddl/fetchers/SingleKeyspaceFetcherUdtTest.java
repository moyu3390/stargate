package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableMap;
import io.stargate.proto.Schema;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.ddl.DdlTestBase;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SingleKeyspaceFetcherUdtTest extends DdlTestBase {

  @Override
  protected Map<String, Schema.CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableMap.of("udts", SampleKeyspaces.UDTS);
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL and generate expected JSON response")
  public void frozenListTest(String query, String expectedJson) {
    assertResponse(query, expectedJson);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "query { keyspace(name:\"udts\") { types { name } } }",
          "{\"keyspace\":{\"types\":[{\"name\":\"A\"}, {\"name\":\"B\"}]}}"),
      arguments(
          "query { keyspace(name: \"udts\") { type(name: \"B\") { "
              + "name,"
              + "fields {name, "
              + "        type {basic}}}}}",
          "{\"keyspace\":{ \"type\":{"
              + "  \"name\":\"B\", "
              + "  \"fields\":["
              + "    {\"name\":\"i\", \"type\":{\"basic\":\"INT\"}}]}}}"),
      arguments(
          "query { keyspace(name: \"udts\") { type(name: \"A\") { "
              + "name,"
              + "fields {name, "
              + "        type {basic, info { name, frozen } }}}}}",
          "{\"keyspace\":{ \"type\":{"
              + "  \"name\":\"A\", "
              + "  \"fields\":["
              + "    {\"name\":\"b\", \"type\":{\"basic\":\"UDT\", \"info\":{\"name\":\"B\", \"frozen\":true}}}]}}}"),
    };
  }
}
