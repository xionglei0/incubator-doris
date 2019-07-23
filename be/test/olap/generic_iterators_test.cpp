// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/generic_iterators.h"

#include <vector>

#include <gtest/gtest.h>

#include "olap/olap_common.h"
#include "olap/schema2.h"
#include "olap/row_block2.h"
#include "util/slice.h"

namespace doris {

class GenericIteratorsTest : public testing::Test {
public:
    GenericIteratorsTest() { }
    virtual ~GenericIteratorsTest() {
    }
};

SchemaV2 create_schema() {
    std::vector<ColumnSchemaV2> col_schemas;

    // c1: small int
    {
        FieldInfo info;
        auto type = get_type_info(OLAP_FIELD_TYPE_SMALLINT);
        info.length = type->size();
        info.type = type->type();
        info.is_allow_null = true;
        col_schemas.push_back(info);
    }
    // c2: int
    {
        FieldInfo info;
        auto type = get_type_info(OLAP_FIELD_TYPE_INT);
        info.length = type->size();
        info.type = type->type();
        info.is_allow_null = true;
        col_schemas.push_back(info);
    }
    // c3: big int
    {
        FieldInfo info;
        auto type = get_type_info(OLAP_FIELD_TYPE_BIGINT);
        info.length = type->size();
        info.type = type->type();
        info.is_allow_null = true;
        col_schemas.push_back(info);
    }

    SchemaV2 schema(col_schemas, 2);
    return schema;
}

TEST(GenericIteratorsTest, AutoIncrement) {
    auto schema = create_schema();
    auto iter = new_auto_increment_iterator(schema, 500);

    StorageReadOptions opts;
    auto st = iter->init(opts);
    ASSERT_TRUE(st.ok());

    Arena arena;
    RowBlockV2 block(schema, 128, &arena);

    size_t row_count = 0;
    do {
        st = iter->next_batch(&block);
        for (int i = 0; i < block.num_rows(); ++i) {
            auto row = block.row(i);
            ASSERT_EQ(row_count, *(int16_t*)row.cell_ptr(0));
            ASSERT_EQ(row_count + 1, *(int32_t*)row.cell_ptr(1));
            ASSERT_EQ(row_count + 2, *(int64_t*)row.cell_ptr(2));
            row_count++;
        }
    } while (st.ok());
    ASSERT_TRUE(st.is_end_of_file());
    ASSERT_EQ(500, row_count);

    delete iter;
}

TEST(GenericIteratorsTest, Union) {
    auto schema = create_schema();
    std::vector<RowwiseIterator*> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_union_iterator(std::move(inputs));
    StorageReadOptions opts;
    auto st = iter->init(opts);
    ASSERT_TRUE(st.ok());

    Arena arena;
    RowBlockV2 block(schema, 128, &arena);

    size_t row_count = 0;
    do {
        st = iter->next_batch(&block);
        for (int i = 0; i < block.num_rows(); ++i) {
            size_t base_value = row_count;
            if (row_count >= 300) {
                base_value -= 300;
            } else if (row_count >= 100) {
                base_value -= 100;
            }
            auto row = block.row(i);
            ASSERT_EQ(base_value, *(int16_t*)row.cell_ptr(0));
            ASSERT_EQ(base_value + 1, *(int32_t*)row.cell_ptr(1));
            ASSERT_EQ(base_value + 2, *(int64_t*)row.cell_ptr(2));
            row_count++;
        }
    } while (st.ok());
    ASSERT_TRUE(st.is_end_of_file());
    ASSERT_EQ(600, row_count);

    delete iter;
}

TEST(GenericIteratorsTest, Merge) {
    auto schema = create_schema();
    std::vector<RowwiseIterator*> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_merge_iterator(std::move(inputs));
    StorageReadOptions opts;
    auto st = iter->init(opts);
    ASSERT_TRUE(st.ok());

    Arena arena;
    RowBlockV2 block(schema, 128, &arena);

    size_t row_count = 0;
    do {
        st = iter->next_batch(&block);
        for (int i = 0; i < block.num_rows(); ++i) {
            size_t base_value = 0;
            // 100 * 3, 200 * 2, 300
            if (row_count < 300) {
                base_value = row_count / 3;
            } else if (row_count < 500) {
                base_value = (row_count - 300) / 2 + 100;
            } else {
                base_value = row_count - 300;
            }
            auto row = block.row(i);
            ASSERT_EQ(base_value, *(int16_t*)row.cell_ptr(0));
            ASSERT_EQ(base_value + 1, *(int32_t*)row.cell_ptr(1));
            ASSERT_EQ(base_value + 2, *(int64_t*)row.cell_ptr(2));
            row_count++;
        }
    } while (st.ok());
    ASSERT_TRUE(st.is_end_of_file());
    ASSERT_EQ(600, row_count);

    delete iter;
}

}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv); 
    return RUN_ALL_TESTS();
}
