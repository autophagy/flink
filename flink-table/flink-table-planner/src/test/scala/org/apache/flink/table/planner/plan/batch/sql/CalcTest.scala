/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.utils.MyPojo
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.NonDeterministicUdf
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.types.AbstractDataType

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.{BeforeEach, Test}

import java.sql.{Date, Time, Timestamp}

class CalcTest extends TableTestBase {

  private val util = batchTestUtil()

  @BeforeEach
  def setup(): Unit = {
    util.addTableSource[(Long, Int, String)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("random_udf", new NonDeterministicUdf)
  }

  @Test
  def testOnlyProject(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable")
  }

  @Test
  def testProjectWithNaming(): Unit = {
    util.verifyExecPlan("SELECT `1-_./Ü`, b, c FROM (SELECT a as `1-_./Ü`, b, c FROM MyTable)")
  }

  @Test
  def testMultiProjects(): Unit = {
    util.verifyExecPlan("SELECT c FROM (SELECT a, c FROM MyTable)")
  }

  @Test
  def testOnlyFilter(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable WHERE b > 0")
  }

  @Test
  def testDisjunctiveFilter(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable WHERE a < 10 OR a > 20")
  }

  @Test
  def testConjunctiveFilter(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable WHERE a < 10 AND b > 20")
  }

  @Test
  def testMultiFilters(): Unit = {
    util.verifyExecPlan("SELECT * FROM (SELECT * FROM MyTable WHERE b > 0) t WHERE a < 50")
  }

  @Test
  def testProjectAndFilter(): Unit = {
    util.verifyExecPlan("SELECT a, b + 1 FROM MyTable WHERE b > 2")
  }

  @Test
  def testIn(): Unit = {
    val sql = s"SELECT * FROM MyTable WHERE b IN (1, 3, 4, 5, 6) AND c = 'xx'"
    util.verifyExecPlan(sql)
  }

  @Test
  def testInNonConstantValues(): Unit = {
    val sql = s"SELECT * FROM MyTable WHERE b IN (1, 3, CAST(a AS INT), 5, 6) AND c = 'xx'"
    util.verifyExecPlan(sql)
  }

  @Test
  def testNotIn(): Unit = {
    val sql = s"SELECT * FROM MyTable WHERE b NOT IN (1, 3, 4, 5, 6) OR c = 'xx'"
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleFlattening(): Unit = {
    util.addTableSource[((Int, Long), (String, Boolean), String)]("MyTable2", 'a, 'b, 'c)
    util.verifyExecPlan("SELECT MyTable2.a.*, c, MyTable2.b.* FROM MyTable2")
  }

  @Test
  def testInvalidFields(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => util.tableEnv.sqlQuery("SELECT a, foo FROM MyTable"))
  }

  @Test
  def testPrimitiveMapType(): Unit = {
    util.verifyExecPlan("SELECT MAP[b, 30, 10, a] FROM MyTable")
  }

  @Test
  def testNonPrimitiveMapType(): Unit = {
    util.verifyExecPlan("SELECT MAP[a, c] FROM MyTable")
  }

  @Test
  def testRowType(): Unit = {
    util.verifyExecPlan("SELECT ROW(1, 'Hi', a) FROM MyTable")
  }

  @Test
  def testArrayType(): Unit = {
    util.verifyExecPlan("SELECT ARRAY['Hi', 'Hello', c] FROM MyTable")
  }

  @Test
  def testProjectWithDateType(): Unit = {
    val sql =
      """
        |SELECT a, b, c,
        | DATE '1984-07-12',
        | TIME '14:34:24',
        | TIMESTAMP '1984-07-12 14:34:24'
        |FROM MyTable
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testFilterWithDateType(): Unit = {
    util.addTableSource[(Long, Date, Time, Timestamp)]("MyTable3", 'a, 'b, 'c, 'd)
    val sql =
      """
        |SELECT * FROM MyTable3
        |WHERE b = DATE '1984-07-12' AND c = TIME '14:34:24' AND d = TIMESTAMP '1984-07-12 14:34:24'
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testPojoType(): Unit = {
    util.addTableSource(
      "MyTable4",
      Array[AbstractDataType[_]](DataTypes.STRUCTURED(classOf[MyPojo])),
      Array("a"))
    util.verifyExecPlan("SELECT a FROM MyTable4")
  }

  @Test
  def testMixedType(): Unit = {
    util.addTableSource[(String, Int, Timestamp)]("MyTable5", 'a, 'b, 'c)
    util.verifyExecPlan(
      "SELECT ROW(a, b, c), ARRAY[12, b], MAP[a, c] FROM MyTable5 " +
        "WHERE (a, b, c) = ('foo', 12, TIMESTAMP '1984-07-12 14:34:24')")
  }

  @Test
  def testCollationDeriveOnCalc(): Unit = {
    util.verifyExecPlan("SELECT CAST(a AS INT), CAST(b AS VARCHAR) FROM (VALUES (3, 'c')) T(a,b)")
  }

  @Test
  def testOrWithIsNullPredicate(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable WHERE a = 1 OR a = 10 OR a IS NULL")
  }

  @Test
  def testOrWithIsNullInIf(): Unit = {
    util.verifyExecPlan("SELECT IF(c = '' OR c IS NULL, 'a', 'b') FROM MyTable")
  }

  @Test
  def testDecimalArrayWithDifferentPrecision(): Unit = {
    util.verifyExecPlan("SELECT ARRAY[0.12, 0.5, 0.99]")
  }

  @Test
  def testDecimalMapWithDifferentPrecision(): Unit = {
    util.verifyExecPlan("SELECT MAP['a', 0.12, 'b', 0.5]")
  }

  @Test
  def testCalcMergeWithNonDeterministicExpr(): Unit = {
    val sqlQuery = "SELECT a, a1 FROM (SELECT a, random_udf(b) AS a1 FROM MyTable) t WHERE a1 > 10"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithNonDeterministicExpr2(): Unit = {
    val sqlQuery = "SELECT a FROM (SELECT a, b FROM MyTable) t WHERE random_udf(b) > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCalcMergeWithCorrelate(): Unit = {
    util.addTemporarySystemFunction("str_split", new StringSplit())
    val sqlQuery =
      """
        |SELECT a, r FROM (
        | SELECT a, random_udf(b) r FROM (
        |  select a, b, c1 FROM MyTable, LATERAL TABLE(str_split(c)) AS T(c1)
        | ) t
        |)
        |WHERE r > 10
        |""".stripMargin
    util.verifyRelPlan(sqlQuery)
  }
}
