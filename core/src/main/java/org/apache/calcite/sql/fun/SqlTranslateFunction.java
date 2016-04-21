/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Definition of the "TRANSLATE" builtin SQL function.
 */
public class SqlTranslateFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates the SqlTranslateFunction.
   */
  SqlTranslateFunction() {
    super(
        "TRANSLATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_VARYING,
        null,
        null,
        SqlFunctionCategory.STRING);
  }

  //~ Methods ----------------------------------------------------------------

  //Check logic
  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    boolean isTranslate3 = 3 == call.operandCount();
    if (isTranslate3) {
      writer.sep(",");
    }
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(isTranslate3 ? "," : "USING");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    if (isTranslate3) {
      writer.sep(",");
      call.operand(2).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }

  public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 2:
      return "{0}({1} USING {2})";
    case 3:
      return "{0}({1}, {2}, {3})";
    default:
      throw new AssertionError();
    }
  }

  public boolean checkOperandTypes(
          SqlCallBinding callBinding,
          boolean throwOnFailure) {
    SqlValidator validator = callBinding.getValidator();
    SqlValidatorScope scope = callBinding.getScope();

    final List<SqlNode> operands = callBinding.operands();
    int n = operands.size();
    assert (3 == n) || (2 == n);
    if (!OperandTypes.STRING.checkSingleOperandType(
            callBinding,
            operands.get(0),
            0,
            throwOnFailure)) {
      return false;
    }
    if (2 == n) {
      if (!OperandTypes.STRING.checkSingleOperandType(
              callBinding,
              operands.get(1),
              0,
              throwOnFailure)) {
        return false;
      }
    } else {
      RelDataType t1 = validator.deriveType(scope, operands.get(1));
      RelDataType t2 = validator.deriveType(scope, operands.get(2));

      if (SqlTypeUtil.inCharFamily(t1)) {
        if (!OperandTypes.STRING.checkSingleOperandType(
                callBinding,
                operands.get(1),
                0,
                throwOnFailure)) {
          return false;
        }
        if (!OperandTypes.STRING.checkSingleOperandType(
                callBinding,
                operands.get(2),
                0,
                throwOnFailure)) {
          return false;
        }

        if (!SqlTypeUtil.isCharTypeComparable(callBinding, operands,
                throwOnFailure)) {
          return false;
        }
      } else {
        if (!OperandTypes.STRING.checkSingleOperandType(
                callBinding,
                operands.get(1),
                0,
                throwOnFailure)) {
          return false;
        }
        if (!OperandTypes.STRING.checkSingleOperandType(
                callBinding,
                operands.get(2),
                0,
                throwOnFailure)) {
          return false;
        }
      }

      if (!SqlTypeUtil.inSameFamily(t1, t2)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }
    }
    return true;
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

}

// End SqlTranslateFunction.java
