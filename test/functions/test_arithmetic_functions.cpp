#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/arithmetic_functions.hpp"
#include "functions/function_registry.hpp"
#include <vector>
#include <cmath>

using namespace tsdb::functions;
using ::testing::_;
using ::testing::DoubleNear;
using ::testing::ElementsAre;

class ArithmeticFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear registry for clean tests
        FunctionRegistry::getInstance().clear();
        
        // Register arithmetic functions
        FunctionRegistry::getInstance().registerFunction<AddFunction>(AddFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<SubtractFunction>(SubtractFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<MultiplyFunction>(MultiplyFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<DivideFunction>(DivideFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<ScaleFunction>(ScaleFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<OffsetFunction>(OffsetFunction::metadata_);
        
        setupTestData();
    }
    
    void TearDown() override {
        FunctionRegistry::getInstance().clear();
    }
    
    void setupTestData() {
        timestamps1 = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
        values1 = {1.0, 2.0, 3.0, 4.0, 5.0};
        
        timestamps2 = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
        values2 = {2.0, 4.0, 6.0, 8.0, 10.0};
        
        timestampsBool = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
        valuesBool = {true, false, true, true, false};
    }
    
    FunctionContext createContext(const std::map<std::string, ParameterValue>& params = {}) {
        FunctionContext context;
        for (const auto& param : params) {
            context.setParameter(param.first, param.second);
        }
        return context;
    }
    
    void expectVectorNear(const std::vector<double>& actual, const std::vector<double>& expected) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            EXPECT_NEAR(actual[i], expected[i], 1e-10) << "at index " << i;
        }
    }
    
    std::vector<uint64_t> timestamps1, timestamps2, timestampsBool;
    std::vector<double> values1, values2;
    std::vector<bool> valuesBool;
};

// Test AddFunction
TEST_F(ArithmeticFunctionsTest, AddFunctionBasic) {
    AddFunction addFunc;
    DoubleSeriesView series1(&timestamps1, &values1);
    FunctionContext context = createContext({{"operand", 10.0}});
    
    auto result = addFunc.execute(series1, context).get();
    
    EXPECT_EQ(result.size(), 5);
    expectVectorNear(result.values, {11.0, 12.0, 13.0, 14.0, 15.0});
    EXPECT_EQ(result.timestamps, timestamps1);
}

TEST_F(ArithmeticFunctionsTest, AddFunctionNegativeOperand) {
    AddFunction addFunc;
    DoubleSeriesView series1(&timestamps1, &values1);
    
    FunctionContext context = createContext({{"operand", -2.0}});
    auto result = addFunc.execute(series1, context).get();
    
    EXPECT_EQ(result.size(), 5);
    expectVectorNear(result.values, {-1.0, 0.0, 1.0, 2.0, 3.0});
    EXPECT_EQ(result.timestamps, timestamps1);
}

TEST_F(ArithmeticFunctionsTest, AddFunctionParameterValidation) {
    AddFunction addFunc;
    
    // Test invalid alignment parameter
    FunctionContext invalidContext = createContext({{"alignment", std::string("invalid")}});
    auto validationResult = addFunc.validateParameters(invalidContext);
    EXPECT_FALSE(validationResult.get());
    
    // Test valid parameters
    FunctionContext validContext = createContext({{"operand", 5.0}, {"alignment", std::string("inner")}});
    validationResult = addFunc.validateParameters(validContext);
    EXPECT_TRUE(validationResult.get());
}

// Test SubtractFunction
TEST_F(ArithmeticFunctionsTest, SubtractFunctionBasic) {
    SubtractFunction subtractFunc;
    DoubleSeriesView series1(&timestamps1, &values1);
    
    FunctionContext context = createContext({{"operand", 1.0}});
    auto result = subtractFunc.execute(series1, context).get();
    
    EXPECT_EQ(result.size(), 5);
    expectVectorNear(result.values, {0.0, 1.0, 2.0, 3.0, 4.0});
    EXPECT_EQ(result.timestamps, timestamps1);
}

// Test MultiplyFunction
TEST_F(ArithmeticFunctionsTest, MultiplyFunctionBasic) {
    MultiplyFunction multiplyFunc;
    DoubleSeriesView series1(&timestamps1, &values1);
    
    FunctionContext context = createContext({{"factor", 2.0}});
    auto result = multiplyFunc.execute(series1, context).get();
    
    EXPECT_EQ(result.size(), 5);
    expectVectorNear(result.values, {2.0, 4.0, 6.0, 8.0, 10.0});
    EXPECT_EQ(result.timestamps, timestamps1);
}

// Test DivideFunction
TEST_F(ArithmeticFunctionsTest, DivideFunctionBasic) {
    DivideFunction divideFunc;
    DoubleSeriesView series1(&timestamps1, &values1);
    
    FunctionContext context = createContext({{"divisor", 2.0}});
    auto result = divideFunc.execute(series1, context).get();
    
    EXPECT_EQ(result.size(), 5);
    expectVectorNear(result.values, {0.5, 1.0, 1.5, 2.0, 2.5});
    EXPECT_EQ(result.timestamps, timestamps1);
}

TEST_F(ArithmeticFunctionsTest, DivideFunctionZeroDivisor) {
    DivideFunction divideFunc;
    DoubleSeriesView series1(&timestamps1, &values1);
    
    FunctionContext context = createContext({{"divisor", 0.0}});
    EXPECT_THROW(divideFunc.execute(series1, context).get(), ParameterValidationException);
}

// Test function metadata
TEST_F(ArithmeticFunctionsTest, FunctionMetadata) {
    AddFunction addFunc;
    const auto& metadata = addFunc.getMetadata();
    
    EXPECT_EQ(metadata.name, "add");
    EXPECT_EQ(metadata.category, FunctionCategory::ARITHMETIC);
    EXPECT_TRUE(metadata.supportsVectorization);
    EXPECT_TRUE(metadata.supportsStreaming);
    EXPECT_GE(metadata.minDataPoints, 1);
    EXPECT_FALSE(metadata.examples.empty());
    
    // Test parameter definitions
    bool hasAlignmentParam = false;
    for (const auto& param : metadata.parameters) {
        if (param.name == "alignment") {
            hasAlignmentParam = true;
            EXPECT_EQ(param.type, "string");
            EXPECT_FALSE(param.required);
            break;
        }
    }
    EXPECT_TRUE(hasAlignmentParam);
}

TEST_F(ArithmeticFunctionsTest, FunctionCloning) {
    AddFunction addFunc;
    auto cloned = addFunc.clone();
    
    EXPECT_NE(cloned.get(), &addFunc);
    EXPECT_EQ(cloned->getName(), addFunc.getName());
}

// Test ScaleFunction
TEST_F(ArithmeticFunctionsTest, ScaleFunctionBasic) {
    ScaleFunction scaleFunc;
    DoubleSeriesView series1(&timestamps1, &values1);
    
    FunctionContext context = createContext({{"factor", 3.0}});
    auto result = scaleFunc.execute(series1, context).get();
    
    EXPECT_EQ(result.size(), 5);
    expectVectorNear(result.values, {3.0, 6.0, 9.0, 12.0, 15.0});
    EXPECT_EQ(result.timestamps, timestamps1);
}

// Test OffsetFunction
TEST_F(ArithmeticFunctionsTest, OffsetFunctionBasic) {
    OffsetFunction offsetFunc;
    DoubleSeriesView series1(&timestamps1, &values1);
    
    FunctionContext context = createContext({{"value", 100.0}});
    auto result = offsetFunc.execute(series1, context).get();
    
    EXPECT_EQ(result.size(), 5);
    expectVectorNear(result.values, {101.0, 102.0, 103.0, 104.0, 105.0});
    EXPECT_EQ(result.timestamps, timestamps1);
}

// main() function removed to avoid multiple definitions when linking with other test files