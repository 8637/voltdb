/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#include <iostream>
#include <limits.h>
#include "harness.h"

#include "expressions/abstractexpression.h"
#include "expressions/expressions.h"
#include "common/common.h"
#include "common/valuevector.h"
#include "common/ValueFactory.hpp"
#include "common/types.h"
#include "common/ValuePeeker.hpp"
#include "common/PlannerDomValue.h"
#include "common/NValue.hpp"
#include "common/SQLException.h"
#include "expressions/expressions.h"
#include "expressions/expressionutil.h"
#include "expressions/functionexpression.h"
#include "expressions/constantvalueexpression.h"

using namespace voltdb;

static bool staticVerboseFlag = false;

struct FunctionTest : public Test {
        FunctionTest() :
                Test(),
                m_pool(),
                m_executorContext(0,
                                  0,
                                  (UndoQuantum *)0,
                                  (Topend *)0,
                                  &m_pool,
                                  (VoltDBEngine *)0,
                                  "localhost",
                                  0,
                                  (DRTupleStream *)0,
                                  (DRTupleStream *)0) {}
        /**
         * A template for calling unary function call expressions.  For any C++
         * type T, define the function "NValue getSomeValue(T val)" to
         * convert the T value to an NValue below.
         */
        template <typename INPUT_TYPE, typename OUTPUT_TYPE>
        int testUnary(int operation, INPUT_TYPE input, OUTPUT_TYPE output, bool expect_null = false);

        /**
         * A template for calling binary function call expressions.  For any C++
         * type T, define the function "NValue getSomeValue(T val)" to
         * convert the T value to an NValue below.
         */
        template <typename LEFT_INPUT_TYPE, typename RIGHT_INPUT_TYPE, typename OUTPUT_TYPE>
        int testBinary(int operation, LEFT_INPUT_TYPE left_input, RIGHT_INPUT_TYPE right_input, OUTPUT_TYPE output, bool expect_null = false);

        static const int64_t BIGINT_SIZE = int64_t(sizeof(int64_t) * CHAR_BIT);
private:
        Pool            m_pool;
        ExecutorContext m_executorContext;
};

static NValue getSomeValue(const std::string &val)
{
    return ValueFactory::getTempStringValue(val);
}

static NValue getSomeValue(const int64_t val)
{
    return ValueFactory::getBigIntValue(val);
}

/**
 * Test a unary function call expression.
 * @returns: -1 if the result of the function evaluation is less than the expected result.
 *            0 if the result of the function evaluation is as expected.
 *            1 if the result of the function evaluation is greater than the expected result.
 * Note that this function may throw an exception from the call to AbstractExpression::eval().
 */
template <typename INPUT_TYPE, typename OUTPUT_TYPE>
int FunctionTest::testUnary(int operation, INPUT_TYPE input, OUTPUT_TYPE output, bool expect_null) {
    std::vector<AbstractExpression *> *argument = new std::vector<AbstractExpression *>();
    ConstantValueExpression *const_val_exp = new ConstantValueExpression(getSomeValue(input));
    argument->push_back(const_val_exp);
    AbstractExpression* bin_exp = ExpressionUtil::functionFactory(operation, argument);
    int cmpout;
    NValue expected = getSomeValue(output);
    NValue answer;
    try {
        answer = bin_exp->eval();
        if (expect_null) {
            // An unexpected non-null can return any non-0. Arbitrarily return 1 as if (answer > expected).
            cmpout = answer.isNull() ? 0 : 1;
        } else {
            cmpout = answer.compare(expected);
        }
    } catch (SQLException &ex) {
        delete bin_exp;
        expected.free();
        throw;
    }
    if (staticVerboseFlag) {
        std::cout << "input: " << std::hex << input
                  << ", answer: \"" << answer.debug() << "\""
                  << ", expected: \"" << (expect_null ? "<NULL>" : expected.debug()) << "\""
                  << ", comp:     " << std::dec << cmpout << "\n";
    }
    return cmpout;
}
/**
 * Test a binary function call expression.
 * @returns: -1 if the result of the function evaluation is less than the expected result.
 *            0 if the result of the function evaluation is as expected.
 *            1 if the result of the function evaluation is greater than the expected result.
 * Note that this function may throw an exception from the call to AbstractExpression::eval().
 */
template <typename LEFT_INPUT_TYPE, typename RIGHT_INPUT_TYPE, typename OUTPUT_TYPE>
int FunctionTest::testBinary(int operation, LEFT_INPUT_TYPE linput, RIGHT_INPUT_TYPE rinput, OUTPUT_TYPE output, bool expect_null) {
    std::vector<AbstractExpression *> *argument = new std::vector<AbstractExpression *>();
    ConstantValueExpression *lhsexp = new ConstantValueExpression(getSomeValue(linput));
    ConstantValueExpression *rhsexp = new ConstantValueExpression(getSomeValue(rinput));
    argument->push_back(lhsexp);
    argument->push_back(rhsexp);

    NValue expected = getSomeValue(output);
    AbstractExpression* bin_exp = ExpressionUtil::functionFactory(operation, argument);
    int cmpout;
    NValue answer;
    try {
        answer = bin_exp->eval();
        if (expect_null) {
            // An unexpected non-null can return any non-0. Arbitrarily return 1 as if (answer > expected).
            cmpout = answer.isNull() ? 0 : 1;
        } else {
            cmpout = answer.compare(expected);
        }
        if (staticVerboseFlag) {
            std::cout << std::hex << "input: test(" << linput << ", " << rinput << ")"
                      << ", answer: \"" << answer.debug() << "\""
                      << ", expected: \"" << (expect_null ? "<NULL>" : expected.debug()) << "\""
                      << ", comp:     " << std::dec << cmpout << "\n";
        }
    } catch (SQLException &ex) {
        expected.free();
        delete bin_exp;
        throw;
    }
    return cmpout;
}

TEST_F(FunctionTest, BinTest) {
    ASSERT_EQ(testUnary(FUNC_VOLT_BIN,
                        0xffLL,
                        "11111111"),
              0);
    ASSERT_EQ(testUnary(FUNC_VOLT_BIN,
                        0x0LL,
                        "0"),
              0);
    ASSERT_EQ(testUnary(FUNC_VOLT_BIN, int64_t(0x8000000000000000), "", true), 0);

    // Walking ones.
    std::string expected("1");
    std::string expectedz("1111111111111111111111111111111111111111111111111111111111111111");
    for (int idx = 0; idx < (BIGINT_SIZE-1); idx += 1) {
            int64_t input = 1ULL << idx;
            ASSERT_EQ(testUnary(FUNC_VOLT_BIN,
                                input,
                                expected),
                      0);
            expected = expected + "0";
            expectedz[(BIGINT_SIZE-1) - idx] = '0';
            ASSERT_EQ(testUnary(FUNC_VOLT_BIN,
                                ~input,
                                expectedz),
                      0);
            expectedz[(BIGINT_SIZE-1) - idx] = '1';
    }
}

TEST_F(FunctionTest, HexTest) {
        ASSERT_EQ(testUnary(FUNC_VOLT_HEX,
                            0xffLL,
                            "FF"),
                  0);
        ASSERT_EQ(testUnary(FUNC_VOLT_HEX,
                            0x0LL,
                            "0"),
                  0);
        ASSERT_EQ(testUnary(FUNC_VOLT_HEX, int64_t(0x8000000000000000),"", true),
                    0);
        // Walking ones.
        // Apparently it's unrecommended to reuse std::stringstream,
        // so we allocate ss and ssz both.
        for (int idx = 0; idx < (BIGINT_SIZE-1) ; idx += 1) {
                std::stringstream ss;
                int64_t input = 1ULL << idx;
                ss << std::hex << std::uppercase << input; // decimal_value
                ASSERT_EQ(testUnary(FUNC_VOLT_HEX,
                                    input,
                                    ss.str()),
                          0);
                std::stringstream ssz;
                ssz << std::hex << std::uppercase << ~input;
                ASSERT_EQ(testUnary(FUNC_VOLT_HEX,
                                    ~input,
                                    ssz.str()),
                          0);
        }
}

TEST_F(FunctionTest, BitAndTest) {
    const int64_t allones = 0xFFFFFFFFFFFFFFFFLL;
    const int64_t nullmarker = 0x8000000000000000LL;
    ASSERT_EQ(testBinary(FUNC_BITAND, 0x0LL, 0x0LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITAND, 0x0LL, 0x1LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITAND, 0x1LL, 0x0LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITAND, 0x1LL, 0x1LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITAND, nullmarker, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all ones.
    for (int idx = 0; idx < BIGINT_SIZE; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_BITAND, allones, (1<<idx), (1<<idx)), 0);
    }
}

TEST_F(FunctionTest, BitOrTest) {
    const int64_t allzeros = 0x0;
    const int64_t nullmarker = 0x8000000000000000LL;
    ASSERT_EQ(testBinary(FUNC_BITOR, 0x0LL, 0x0LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITOR, 0x1LL, 0x0LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITOR, 0x0LL, 0x1LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITOR, 0x1LL, 0x1LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITOR, nullmarker, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    for (int idx = 0; idx < BIGINT_SIZE; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_BITOR, allzeros, (1<<idx), (1<<idx)), 0);
    }
}

TEST_F(FunctionTest, BitXorTest) {
    const int64_t allzeros = 0x0LL;
    const int64_t allones = 0xFFFFFFFFFFFFFFFFLL;
    const int64_t nullmarker = 0x8000000000000000LL;
    ASSERT_EQ(testBinary(FUNC_BITXOR, 0x0LL, 0x0LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITXOR, 0x1LL, 0x0LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITXOR, 0x0LL, 0x1LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITXOR, 0x1LL, 0x1LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITXOR, nullmarker, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    for (int idx = 0; idx < BIGINT_SIZE; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_BITXOR, allzeros, (1<<idx), (1<<idx)), 0);
        ASSERT_EQ(testBinary(FUNC_BITXOR, allones, (1<<idx), (allones ^ (1 << idx))), 0);
    }
}

TEST_F(FunctionTest, BitLshTest) {
    const int64_t nullmarker = 0x8000000000000000LL;
    const int64_t one = 0x1LL;
    const int64_t three = 0x3LL;

    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, nullmarker, 0,          0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, nullmarker, 1,          0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, one,        nullmarker, 0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, one,        nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    // Don't put the bit all the way at the left end, though.
    for (int idx = 0; idx < BIGINT_SIZE-1; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, 0x1LL, idx, 0x1LL << idx), 0);
        ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, three, idx, three << idx), 0);
    }
    // Test shifting all the way to the right.
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, three, BIGINT_SIZE - 2, (three << (BIGINT_SIZE-2))), 0);
}

TEST_F(FunctionTest, BitRshTest) {
    const int64_t nullmarker = 0x8000000000000000LL;
    const int64_t maxleftbit = 0x4000000000000000LL;
    const int64_t three = 0x3LL;

    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, nullmarker, 0,          0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, nullmarker, 1,          0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, maxleftbit, nullmarker, 0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, maxleftbit, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    for (int idx = 0; idx < BIGINT_SIZE-1; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_RIGHT, maxleftbit, idx, (maxleftbit >> idx)), 0);
        ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_RIGHT, (three << idx), idx, three), 0);
    }
}

TEST_F(FunctionTest, BitNotTest) {
    const int64_t nullmarker = 0x8000000000000000LL;

    ASSERT_EQ(testUnary(FUNC_VOLT_BITNOT, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    for (int idx = 0; idx < BIGINT_SIZE; idx += 1) {
        ASSERT_EQ(testUnary(FUNC_VOLT_BITNOT, (1<<idx), ~(1<<idx)), 0);
    }
}

TEST_F(FunctionTest, RepeatTooBig) {
    bool sawexception = false;
    try {
        ASSERT_EQ(testBinary(FUNC_REPEAT, "amanaplanacanalpanama", int64_t(1), "amanaplanacanalpanama", false), 0);
    } catch (voltdb::SQLException &ex) {
        sawexception = true;
    }
    ASSERT_FALSE(sawexception);
    sawexception = false;
    try {
        ASSERT_EQ(testBinary(FUNC_REPEAT, "amanaplanacanalpanama", 1000000, "", false), 1);
    } catch (voltdb::SQLException &ex) {
        sawexception = true;
    }
    ASSERT_TRUE(sawexception);
}

int main() {
     return TestSuite::globalInstance()->runAll();
}
