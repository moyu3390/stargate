package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.stargate.grpc.Values;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryFetcherCollectionsTest extends DmlTestBase {

  @Override
  protected List<CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.COLLECTIONS);
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL with collections and generate expected CQL query")
  public void collectionsTest(String graphQlQuery, String expectedCqlQuery) {
    assertQuery(String.format("query { %s }", graphQlQuery), expectedCqlQuery);
  }

  public static Arguments[] successfulQueries() {
    // Note that some of these queries require ALLOW FILTERING or indexes in real life. It doesn't
    // matter here, we're just testing the generation of the query, not executing it.
    return new Arguments[] {
      // List:
      arguments(
          "PkListTable(value: { l: [1,2,3] }) { values { l } }",
          "SELECT l FROM collections.\"PkListTable\" WHERE l = :\"l\"",
          ImmutableMap.of("l", listV(1, 2, 3))),
      arguments(
          "PkListTable(filter: { l: {eq: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.\"PkListTable\" WHERE l = :\"l\"",
          ImmutableMap.of("l", listV(1, 2, 3))),
      arguments(
          "PkListTable(filter: { l: {gt: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.\"PkListTable\" WHERE l > :\"l\"",
          ImmutableMap.of("l", listV(1, 2, 3))),
      arguments(
          "PkListTable(filter: { l: {gte: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.\"PkListTable\" WHERE l >= :\"l\"",
          ImmutableMap.of("l", listV(1, 2, 3))),
      arguments(
          "PkListTable(filter: { l: {lt: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.\"PkListTable\" WHERE l < :\"l\"",
          ImmutableMap.of("l", listV(1, 2, 3))),
      arguments(
          "PkListTable(filter: { l: {lte: [1,2,3] } }) { values { l } }",
          "SELECT l FROM collections.\"PkListTable\" WHERE l <= :\"l\"",
          ImmutableMap.of("l", listV(1, 2, 3))),
      arguments(
          "PkListTable(filter: { l: {in: [[1,2,3],[4,5,6]] } }) { values { l } }",
          "SELECT l FROM collections.\"PkListTable\" WHERE l IN :\"l\"",
          ImmutableMap.of("l", listV(listV(1, 2, 3), listV(2, 3, 4)))),
      arguments(
          "RegularListTable(filter: { l: {contains: 1 } }) { values { l } }",
          "SELECT l FROM collections.\"RegularListTable\" WHERE l CONTAINS :\"l\"",
          ImmutableMap.of("l", Values.of(1))),

      // Set:
      arguments(
          "PkSetTable(value: { s: [1,2,3] }) { values { s } }",
          "SELECT s FROM collections.\"PkSetTable\" WHERE s = :\"s\"",
          ImmutableMap.of("s", listV(1, 2, 3))),
      arguments(
          "PkSetTable(filter: { s: {eq: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.\"PkSetTable\" WHERE s = :\"s\"",
          ImmutableMap.of("s", listV(1, 2, 3))),
      arguments(
          "PkSetTable(filter: { s: {gt: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.\"PkSetTable\" WHERE s > :\"s\"",
          ImmutableMap.of("s", listV(1, 2, 3))),
      arguments(
          "PkSetTable(filter: { s: {gte: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.\"PkSetTable\" WHERE s >= :\"s\"",
          ImmutableMap.of("s", listV(1, 2, 3))),
      arguments(
          "PkSetTable(filter: { s: {lt: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.\"PkSetTable\" WHERE s < :\"s\"",
          ImmutableMap.of("s", listV(1, 2, 3))),
      arguments(
          "PkSetTable(filter: { s: {lte: [1,2,3] } }) { values { s } }",
          "SELECT s FROM collections.\"PkSetTable\" WHERE s <= :\"s\"",
          ImmutableMap.of("s", listV(1, 2, 3))),
      arguments(
          "PkSetTable(filter: { s: {in: [[1,2,3],[4,5,6]] } }) { values { s } }",
          "SELECT s FROM collections.\"PkSetTable\" WHERE s IN :\"s\"",
          ImmutableMap.of("s", listV(listV(1, 2, 3), listV(2, 3, 4)))),
      arguments(
          "RegularSetTable(filter: { s: {contains: 1 } }) { values { s } }",
          "SELECT s FROM collections.\"RegularSetTable\" WHERE s CONTAINS :\"s\"",
          ImmutableMap.of("s", Values.of(1))),

      // Map:
      arguments(
          "PkMapTable(value: { m: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"PkMapTable\" WHERE m = :\"m\"",
          ImmutableMap.of("m", listV(1, "a", 2, "b"))),
      arguments(
          "PkMapTable(filter: { m: { eq: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"PkMapTable\" WHERE m = :\"m\"",
          ImmutableMap.of("m", listV(1, "a", 2, "b"))),
      arguments(
          "PkMapTable(filter: { m: { gt: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"PkMapTable\" WHERE m > :\"m\"",
          ImmutableMap.of("m", listV(1, "a", 2, "b"))),
      arguments(
          "PkMapTable(filter: { m: { gte: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"PkMapTable\" WHERE m >= :\"m\"",
          ImmutableMap.of("m", listV(1, "a", 2, "b"))),
      arguments(
          "PkMapTable(filter: { m: { lt: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"PkMapTable\" WHERE m < :\"m\"",
          ImmutableMap.of("m", listV(1, "a", 2, "b"))),
      arguments(
          "PkMapTable(filter: { m: { lte: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"PkMapTable\" WHERE m <= :\"m\"",
          ImmutableMap.of("m", listV(1, "a", 2, "b"))),
      arguments(
          "PkMapTable(filter: { m: { in: ["
              + "  [{key: 1,value:\"a\"},{key: 2,value:\"b\"}],"
              + "  [{key: 3,value:\"c\"},{key: 4,value:\"d\"}]"
              + "] } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"PkMapTable\" WHERE m IN :\"m\"",
          ImmutableMap.of("m", listV(listV(1, "a", 2, "b"), listV(3, "c", 4, "d")))),
      arguments(
          "RegularMapTable(filter: { m: {containsKey: 1 } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"RegularMapTable\" WHERE m CONTAINS KEY :\"m\"",
          ImmutableMap.of("m", Values.of(1))),
      arguments(
          "RegularMapTable(filter: { m: {contains: \"a\" } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"RegularMapTable\" WHERE m CONTAINS :\"m\"",
          ImmutableMap.of("m", Values.of("a"))),
      arguments(
          "RegularMapTable(filter: { m: {containsEntry: {key: 1,value:\"a\"} } }) { values { m{key,value} } }",
          "SELECT m FROM collections.\"RegularMapTable\" WHERE m[:\"m\"] = :\"m2\"",
          ImmutableMap.of("m", Values.of(1), "m2", Values.of("a"))),

      // Nested collection (map<int, list<set<text>>>):
      arguments(
          "NestedCollections(filter: { c: { eq: ["
              + "  {key: 1, value:[[\"a\"],[\"b\"]]},"
              + "  {key: 2, value:[[\"c\"],[\"d\"]]}"
              + "] } }) "
              + "{ values { k } }",
          "SELECT k FROM collections.\"NestedCollections\" WHERE c = :\"c\"",
          ImmutableMap.of(
              "c",
              listV(
                  1, listV(listV(("a")), listV("b")),
                  2, listV(listV(("c")), listV("d"))))),
      arguments(
          "NestedCollections(filter: { c: { contains: [[\"a\"],[\"b\"]] } })" + "{ values { k } }",
          "SELECT k FROM collections.\"NestedCollections\" WHERE c CONTAINS :\"c\"",
          ImmutableMap.of("c", listV(listV(("a")), listV("b")))),
      arguments(
          "NestedCollections(filter: { c: { containsEntry: {key: 1, value:[[\"a\"],[\"b\"]]} } })"
              + "{ values { k } }",
          "SELECT k FROM collections.\"NestedCollections\" WHERE c[:\"c\"] = :\"c2\"",
          ImmutableMap.of("c", listV(1), "c2", listV(listV(("a")), listV("b")))),
    };
  }
}
