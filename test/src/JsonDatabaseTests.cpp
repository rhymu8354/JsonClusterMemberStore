/**
 * @file JsonDatabaseTests.cpp
 *
 * This module contains the unit tests of the
 * JsonClusterMemberStore class.
 */

#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <JsonClusterMemberStore/JsonDatabase.hpp>
#include <memory>
#include <set>
#include <SystemAbstractions/File.hpp>
#include <SystemAbstractions/IFile.hpp>
#include <SystemAbstractions/IFileSystemEntry.hpp>
#include <unordered_set>

using namespace ClusterMemberStore;

struct DecoratedStore
    : public SystemAbstractions::IFileSystemEntry
{
    // Properties

    std::shared_ptr< SystemAbstractions::File > wrapped;
    bool openForReadOnlyAttempted = false;
    bool openForReadWriteAttempted = false;
    bool completeReadAttempted = false;
    bool writeAttempted = false;
    bool failOnOpenReadOnly = false;
    bool failOnOpenReadWrite = false;
    bool failOnRead = false;
    bool failOnWrite = false;
    bool failOnSetSize = false;
    bool messUpTheJson = false;

    // Methods
public:
    DecoratedStore(std::shared_ptr< SystemAbstractions::File > wrapped)
        : wrapped(wrapped)
    {
    }

    // SystemAbstractions::IFileSystemEntry
public:
    virtual bool IsExisting() override {
        return wrapped->IsExisting();
    }

    virtual bool IsDirectory() override {
        return wrapped->IsDirectory();
    }

    virtual bool OpenReadOnly() override {
        openForReadOnlyAttempted = true;
        if (failOnOpenReadOnly) {
            return false;
        } else {
            return wrapped->OpenReadOnly();
        }
    }

    virtual void Close() override {
        wrapped->Close();
    }

    virtual bool OpenReadWrite() override {
        openForReadWriteAttempted = true;
        if (failOnOpenReadWrite) {
            return false;
        } else {
            return wrapped->OpenReadWrite();
        }
    }

    virtual void Destroy() override {
        wrapped->Destroy();
    }

    virtual bool Move(const std::string& newPath) override {
        return wrapped->Move(newPath);
    }

    virtual bool Copy(const std::string& destination) override {
        return wrapped->Copy(destination);
    }

    virtual time_t GetLastModifiedTime() const override {
        return wrapped->GetLastModifiedTime();
    }

    virtual std::string GetPath() const override {
        return wrapped->GetPath();
    }

    // SystemAbstractions::IFile
public:
    virtual uint64_t GetSize() const override {
        return wrapped->GetSize();
    }

    virtual bool SetSize(uint64_t size) override {
        if (failOnSetSize) {
            return false;
        } else {
            return wrapped->SetSize(size);
        }
    }

    virtual uint64_t GetPosition() const override {
        return wrapped->GetPosition();
    }

    virtual void SetPosition(uint64_t position) override {
        wrapped->SetPosition(position);
    }

    virtual size_t Peek(Buffer& buffer, size_t numBytes = 0, size_t offset = 0) const override {
        return wrapped->Peek(buffer, numBytes, offset);
    }

    virtual size_t Peek(void* buffer, size_t numBytes) const override {
        return wrapped->Peek(buffer, numBytes);
    }

    virtual size_t Read(Buffer& buffer, size_t numBytes = 0, size_t offset = 0) override {
        if (
            (numBytes == wrapped->GetSize())
            || (numBytes == 0)
        ) {
            completeReadAttempted = true;
        }
        if (failOnRead) {
            return 0;
        } else {
            const auto result = wrapped->Read(buffer, numBytes, offset);
            if (messUpTheJson) {
                buffer[0] = '!';
            }
            return result;
        }
    }

    virtual size_t Read(void* buffer, size_t numBytes) override {
        if (numBytes == wrapped->GetSize()) {
            completeReadAttempted = true;
        }
        if (failOnRead) {
            return 0;
        } else {
            const auto result = wrapped->Read(buffer, numBytes);
            if (messUpTheJson) {
                ((char*)buffer)[0] = '!';
            }
            return result;
        }
    }

    virtual size_t Write(const Buffer& buffer, size_t numBytes = 0, size_t offset = 0) override {
        writeAttempted = true;
        if (failOnWrite) {
            return 0;
        } else {
            return wrapped->Write(buffer, numBytes, offset);
        }
    }

    virtual size_t Write(const void* buffer, size_t numBytes) override {
        writeAttempted = true;
        if (failOnWrite) {
            return 0;
        } else {
            return wrapped->Write(buffer, numBytes);
        }
    }

    virtual std::shared_ptr< IFile > Clone() override {
        return wrapped->Clone();
    }

};

/**
 * This is the base for test fixtures used to test the SMTP library.
 */
struct JsonDatabaseTests
    : public ::testing::Test
{
    // Properties

    std::shared_ptr< SystemAbstractions::File > store;
    JsonDatabase db;
    const Json::Value startingSerialization = Json::Object({
        {"schema", Json::Object({
            {"kv", Json::Array({
                Json::Object({
                    {"name", "key"},
                    {"type", "text"},
                    {"key", true},
                }),
                Json::Object({
                    {"name", "value"},
                    {"type", "text"},
                    {"key", false},
                }),
            })},
            {"npcs", Json::Array({
                Json::Object({
                    {"name", "entity"},
                    {"type", "int"},
                    {"key", true},
                }),
                Json::Object({
                    {"name", "name"},
                    {"type", "text"},
                    {"key", false},
                }),
                Json::Object({
                    {"name", "job"},
                    {"type", "text"},
                    {"key", false},
                }),
            })},
            {"quests", Json::Array({
                Json::Object({
                    {"name", "npc"},
                    {"type", "int"},
                    {"key", false},
                }),
                Json::Object({
                    {"name", "quest"},
                    {"type", "int"},
                    {"key", false},
                }),
            })},
        })},
        {"kv", Json::Object({
            {"foo", Json::Object({{"value", "bar"}})},
        })},
        {"npcs", Json::Array({
            Json::Object({
                {"entity", 1},
                {"name", "Alex"},
                {"job", "Armorer"},
            }),
            Json::Object({
                {"entity", 2},
                {"name", "Bob"},
                {"job", "Banker"},
            }),
        })},
        {"quests", Json::Array({
            Json::Object({
                {"npc", 1},
                {"quest", 42},
            }),
            Json::Object({
                {"npc", 1},
                {"quest", 43},
            }),
            Json::Object({
                {"npc", 2},
                {"quest", 43},
            }),
        })},
    });

    // Methods

    JsonDatabaseTests()
        : store(std::make_shared< SystemAbstractions::File >(
            SystemAbstractions::File::GetExeParentDirectory() + "/test.db"
        ))
    {
    }

    /**
     * Read the entire contents of the given file and parse it as JSON,
     * returning the value parsed (if any).
     *
     * @param[in,out] file
     *     This is the file to read.
     *
     * @return
     *     The parsed JSON value is returned.
     *
     * @retval Json::Value()
     *     A default-constructed JSON value (which has a special "invalid"
     *     type) is returned if there is any issue reading or parsing the file.
     */
    Json::Value ReadJsonFromFile(
        SystemAbstractions::File& file
    ) {
        if (!file.OpenReadOnly()) {
            return Json::Value();
        }
        SystemAbstractions::IFile::Buffer buffer(file.GetSize());
        const auto amountRead = file.Read(buffer);
        file.Close();
        if (amountRead != buffer.size()) {
            return Json::Value();
        }
        return Json::Value::FromEncoding(
            std::string(
                buffer.begin(),
                buffer.end()
            )
        );
    }

    /**
     * Encode the given JSON value and write it out to the given file.
     *
     * @param[in,out] file
     *     This is the file to write.
     *
     * @param[in] value
     *     This is the JSON value to encode and write to the file.
     */
    void WriteJsonToFile(
        SystemAbstractions::File& file,
        const Json::Value& value
    ) {
        if (!file.OpenReadWrite()) {
            return;
        }
        Json::EncodingOptions jsonEncodingOptions;
        jsonEncodingOptions.pretty = true;
        jsonEncodingOptions.reencode = true;
        const auto encoding = value.ToEncoding(jsonEncodingOptions);
        SystemAbstractions::IFile::Buffer buffer(
            encoding.begin(),
            encoding.end()
        );
        (void)file.Write(buffer);
        (void)file.SetSize(buffer.size());
        file.Close();
    }

    void VerifySerialization(const Json::Value& serialization) {
        ASSERT_TRUE(db.Commit());
        EXPECT_EQ(
            serialization,
            ReadJsonFromFile(*store)
        );
    }
    void VerifyNoChanges() {
        VerifySerialization(startingSerialization);
    }

    // ::testing::Test

    virtual void SetUp() override {
        store->Destroy();
        WriteJsonToFile(
            *store,
            startingSerialization
        );
        (void)db.Open(store);
    }

    virtual void TearDown() override {
    }

};

TEST_F(JsonDatabaseTests, Open_Successful) {
    // Arrange
    db = JsonDatabase();
    const auto decoratedStore = std::make_shared< DecoratedStore >(store);

    // Act
    const auto openResult = db.Open(decoratedStore);

    // Assert
    EXPECT_TRUE(openResult);
    EXPECT_TRUE(decoratedStore->openForReadOnlyAttempted);
    EXPECT_TRUE(decoratedStore->completeReadAttempted);
}

TEST_F(JsonDatabaseTests, Open_Error_Opening_File) {
    // Arrange
    db = JsonDatabase();
    const auto decoratedStore = std::make_shared< DecoratedStore >(store);
    decoratedStore->failOnOpenReadOnly = true;

    // Act
    const auto openResult = db.Open(decoratedStore);

    // Assert
    EXPECT_FALSE(openResult);
    EXPECT_TRUE(decoratedStore->openForReadOnlyAttempted);
    EXPECT_FALSE(decoratedStore->completeReadAttempted);
}

TEST_F(JsonDatabaseTests, Open_Error_Reading_File) {
    // Arrange
    db = JsonDatabase();
    const auto decoratedStore = std::make_shared< DecoratedStore >(store);
    decoratedStore->failOnRead = true;

    // Act
    const auto openResult = db.Open(decoratedStore);

    // Assert
    EXPECT_FALSE(openResult);
    EXPECT_TRUE(decoratedStore->openForReadOnlyAttempted);
    EXPECT_TRUE(decoratedStore->completeReadAttempted);
}

TEST_F(JsonDatabaseTests, Open_Error_Parsing_File) {
    // Arrange
    db = JsonDatabase();
    const auto decoratedStore = std::make_shared< DecoratedStore >(store);
    decoratedStore->messUpTheJson = true;

    // Act
    const auto openResult = db.Open(decoratedStore);

    // Assert
    EXPECT_FALSE(openResult);
    EXPECT_TRUE(decoratedStore->openForReadOnlyAttempted);
    EXPECT_TRUE(decoratedStore->completeReadAttempted);
}

TEST_F(JsonDatabaseTests, Commit_Without_Open) {
    // Arrange
    db = JsonDatabase();

    // Act
    const auto commitResult = db.Commit();

    // Assert
    EXPECT_FALSE(commitResult);
}


TEST_F(JsonDatabaseTests, Commit_Successful) {
    // Arrange
    db = JsonDatabase();
    const auto decoratedStore = std::make_shared< DecoratedStore >(store);
    (void)db.Open(decoratedStore);

    // Act
    const auto commitResult = db.Commit();

    // Assert
    EXPECT_TRUE(commitResult);
    EXPECT_TRUE(decoratedStore->openForReadWriteAttempted);
    EXPECT_TRUE(decoratedStore->writeAttempted);
}

TEST_F(JsonDatabaseTests, Commit_Error_Opening_File) {
    // Arrange
    db = JsonDatabase();
    const auto decoratedStore = std::make_shared< DecoratedStore >(store);
    (void)db.Open(decoratedStore);
    decoratedStore->failOnOpenReadWrite = true;

    // Act
    const auto commitResult = db.Commit();

    // Assert
    EXPECT_FALSE(commitResult);
    EXPECT_TRUE(decoratedStore->openForReadWriteAttempted);
    EXPECT_FALSE(decoratedStore->writeAttempted);
}

TEST_F(JsonDatabaseTests, Commit_Error_Writing_File) {
    // Arrange
    db = JsonDatabase();
    const auto decoratedStore = std::make_shared< DecoratedStore >(store);
    (void)db.Open(decoratedStore);
    decoratedStore->failOnWrite = true;

    // Act
    const auto commitResult = db.Commit();

    // Assert
    EXPECT_FALSE(commitResult);
    EXPECT_TRUE(decoratedStore->openForReadWriteAttempted);
    EXPECT_TRUE(decoratedStore->writeAttempted);
}

TEST_F(JsonDatabaseTests, Commit_Error_Truncating_File) {
    // Arrange
    db = JsonDatabase();
    const auto decoratedStore = std::make_shared< DecoratedStore >(store);
    (void)db.Open(decoratedStore);
    decoratedStore->failOnSetSize = true;

    // Act
    const auto commitResult = db.Commit();

    // Assert
    EXPECT_FALSE(commitResult);
    EXPECT_TRUE(decoratedStore->openForReadWriteAttempted);
    EXPECT_TRUE(decoratedStore->writeAttempted);
}

TEST_F(JsonDatabaseTests, CreateTable) {
    // Arrange
    TableDefinition tableDefinition;
    tableDefinition.columnDefinitions.push_back({
        "entity", "int", true,
    });
    tableDefinition.columnDefinitions.push_back({
        "favorite_color", "text", false,
    });

    // Act
    db.CreateTable("ktulu", tableDefinition);

    // Assert
    EXPECT_TRUE(db.Commit());
    const auto contents = ReadJsonFromFile(*store);
    EXPECT_EQ(
        Json::Array({
            Json::Object({
                {"name", "entity"},
                {"type", "int"},
                {"key", true},
            }),
            Json::Object({
                {"name", "favorite_color"},
                {"type", "text"},
                {"key", false},
            }),
        }),
        contents["schema"]["ktulu"]
    );
    EXPECT_EQ(
        Json::Array({}),
        contents["ktulu"]
    );
    auto schema = db.DescribeTables();
    EXPECT_EQ(
        TableDefinition({
            {
                {"entity", "int", true},
                {"favorite_color", "text", false},
            },
        }),
        schema["ktulu"]
    );
}

TEST_F(JsonDatabaseTests, DescribeTables) {
    // Arrange

    // Act
    const auto schema = db.DescribeTables();

    // Assert
    EXPECT_EQ(
        TableDefinitions({
            {"kv", {
                {
                    {"key", "text", true},
                    {"value", "text", false},
                },
            }},
            {"npcs", {
                {
                    {"entity", "int", true},
                    {"name", "text", false},
                    {"job", "text", false},
                },
            }},
            {"quests", {
                {
                    {"npc", "int", false},
                    {"quest", "int", false},
                },
            }},
        }),
        schema
    );
}

TEST_F(JsonDatabaseTests, RenameTable_New_Name_Not_In_Use) {
    // Arrange
    const auto& tableSchema = startingSerialization["schema"]["npcs"];
    const auto& tableData = startingSerialization["npcs"];
    const auto npcsSchema = db.DescribeTables()["npcs"];

    // Act
    db.RenameTable("npcs", "people");

    // Assert
    ASSERT_TRUE(db.Commit());
    const auto contents = ReadJsonFromFile(*store);
    EXPECT_EQ(tableSchema, contents["schema"]["people"]);
    EXPECT_FALSE(contents["schema"].Has("npcs"));
    EXPECT_EQ(tableData, contents["people"]);
    EXPECT_FALSE(contents.Has("npcs"));
    auto schema = db.DescribeTables();
    EXPECT_EQ(npcsSchema, schema["people"]);
}

TEST_F(JsonDatabaseTests, RenameTable_New_Name_In_Use) {
    // Arrange
    const auto& npcsTableSchema = startingSerialization["schema"]["npcs"];
    const auto& kvTableSchema = startingSerialization["schema"]["kv"];
    const auto& npcsTableData = startingSerialization["npcs"];
    const auto& kvTableData = startingSerialization["kv"];

    // Act
    db.RenameTable("npcs", "kv");

    // Assert
    ASSERT_TRUE(db.Commit());
    const auto contents = ReadJsonFromFile(*store);
    EXPECT_EQ(npcsTableSchema, contents["schema"]["npcs"]);
    EXPECT_EQ(kvTableSchema, contents["schema"]["kv"]);
    EXPECT_EQ(npcsTableData, contents["npcs"]);
    EXPECT_EQ(kvTableData, contents["kv"]);
}

TEST_F(JsonDatabaseTests, RenameTable_New_Name_Blank) {
    // Arrange
    const auto& tableSchema = startingSerialization["schema"]["npcs"];
    const auto& tableData = startingSerialization["npcs"];

    // Act
    db.RenameTable("npcs", "");

    // Assert
    ASSERT_TRUE(db.Commit());
    const auto contents = ReadJsonFromFile(*store);
    EXPECT_EQ(tableSchema, contents["schema"]["npcs"]);
    EXPECT_FALSE(contents["schema"].Has(""));
    EXPECT_EQ(tableData, contents["npcs"]);
    EXPECT_FALSE(contents.Has(""));
}

TEST_F(JsonDatabaseTests, RenameTable_Old_Name_Missing) {
    // Arrange

    // Act
    db.RenameTable("foo", "bar");

    // Assert
    ASSERT_TRUE(db.Commit());
    const auto contents = ReadJsonFromFile(*store);
    EXPECT_FALSE(contents["schema"]["bar"]);
    EXPECT_FALSE(contents["bar"]);
}

TEST_F(JsonDatabaseTests, AddColumn_Existing_Table) {
    // Arrange
    const auto numNpcsColumns = startingSerialization["schema"]["npcs"].GetSize();
    auto expectedSerialization = startingSerialization;
    expectedSerialization["schema"]["npcs"].Add(
        Json::Object({
            {"name", "hp"},
            {"type", "int"},
            {"key", false},
        })
    );
    const ColumnDefinition newColumn{"hp", "int", false};

    // Act
    db.AddColumn("npcs", newColumn);

    // Assert
    VerifySerialization(expectedSerialization);
    auto schema = db.DescribeTables();
    EXPECT_EQ(
        newColumn,
        schema["npcs"].columnDefinitions[numNpcsColumns]
    );
}

TEST_F(JsonDatabaseTests, AddColumn_No_Such_Table) {
    // Arrange

    // Act
    db.AddColumn("foobar", {"hp", "int", false});

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, DestroyColumn_Table_And_Column_Exists) {
    // Arrange
    const auto numNpcsColumns = startingSerialization["schema"]["npcs"].GetSize();
    auto expectedSerialization = startingSerialization;
    auto& expectedNpcSchema = expectedSerialization["schema"]["npcs"];
    for (size_t i = 0; i < expectedNpcSchema.GetSize(); ++i) {
        if (expectedNpcSchema[i]["name"] == "job") {
            expectedNpcSchema.Remove(i);
            break;
        }
    }
    auto& expectedSerializationNpcs = expectedSerialization["npcs"];
    for (size_t i = 0; i < expectedSerializationNpcs.GetSize(); ++i) {
        expectedSerializationNpcs[i].Remove("job");
    }

    // Act
    db.DestroyColumn("npcs", "job");

    // Assert
    VerifySerialization(expectedSerialization);
}

TEST_F(JsonDatabaseTests, DestroyColumn_No_Such_Table) {
    // Arrange

    // Act
    db.DestroyColumn("foobar", "job");

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, DestroyColumn_No_Such_Column) {
    // Arrange

    // Act
    db.DestroyColumn("npcs", "magic");

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, DestroyTable_Table_Which_Existed) {
    // Arrange
    auto expectedSerialization = startingSerialization;
    expectedSerialization["schema"].Remove("npcs");
    expectedSerialization.Remove("npcs");

    // Act
    db.DestroyTable("npcs");

    // Assert
    VerifySerialization(expectedSerialization);
}

TEST_F(JsonDatabaseTests, DestroyTable_Table_No_Such_Table) {
    // Arrange

    // Act
    db.DestroyTable("foobar");

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, CreateRow_No_Such_Table) {
    // Arrange

    // Act
    Value entity, name;
    entity.data.integer = 3;
    entity.type = DataType::Integer;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    db.CreateRow("does_not_exist", {
        {"entity", std::move(entity)},
        {"name", std::move(name)},
    });

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, CreateRow_Not_String_Key_And_Key_Is_New) {
    // Arrange
    auto expectedSerialization = startingSerialization;
    expectedSerialization["npcs"].Add(
        Json::Object({
            {"entity", 3},
            {"name", "Steve"},
        })
    );

    // Act
    //
    // By the way, we're also going to test trying to insert data
    // for a column (foo) that doesn't exist in the table.
    Value entity, name, foo;
    entity.data.integer = 3;
    entity.type = DataType::Integer;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    std::string fooStorage = "bar";
    foo.data.text = &fooStorage;
    foo.type = DataType::Text;
    db.CreateRow("npcs", {
        {"entity", std::move(entity)},
        {"name", std::move(name)},
        {"foo", std::move(foo)},
    });

    // Assert
    VerifySerialization(expectedSerialization);
}

TEST_F(JsonDatabaseTests, CreateRow_Not_String_Key_And_Key_Missing) {
    // Arrange

    // Act
    Value name;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    db.CreateRow("npcs", {
        {"name", std::move(name)},
    });

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, CreateRow_Not_String_Key_And_Key_Already_Exists_In_Table) {
    // Arrange

    // Act
    Value entity, name;
    entity.data.integer = 2;
    entity.type = DataType::Integer;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    db.CreateRow("npcs", {
        {"entity", std::move(entity)},
        {"name", std::move(name)},
    });

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, CreateRow_String_Key_And_Key_Missing) {
    // Arrange

    // Act
    Value value;
    std::string valueStorage = "superstar";
    value.data.text = &valueStorage;
    value.type = DataType::Text;
    db.CreateRow("kv", {
        {"value", std::move(value)},
    });

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, CreateRow_String_Key_And_Key_Is_New) {
    // Arrange
    auto expectedSerialization = startingSerialization;
    expectedSerialization["kv"]["Gaddam"] = Json::Object({
        {"value", "superstar"},
    });

    // Act
    Value key, value;
    std::string keyStorage = "Gaddam";
    key.data.text = &keyStorage;
    key.type = DataType::Text;
    std::string valueStorage = "superstar";
    value.data.text = &valueStorage;
    value.type = DataType::Text;
    db.CreateRow("kv", {
        {"key", std::move(key)},
        {"value", std::move(value)},
    });

    // Assert
    VerifySerialization(expectedSerialization);
}

TEST_F(JsonDatabaseTests, CreateRow_String_Key_And_Key_Already_Exists_In_Table) {
    // Arrange

    // Act
    Value key, value;
    std::string keyStorage = "foo";
    key.data.text = &keyStorage;
    key.type = DataType::Text;
    std::string valueStorage = "spam";
    value.data.text = &valueStorage;
    value.type = DataType::Text;
    db.CreateRow("kv", {
        {"key", std::move(key)},
        {"value", std::move(value)},
    });

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, RetrieveRows_No_Such_Table) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::All;
    const auto data = db.RetrieveRows(
        "foobar",
        rowSelector,
        {"quest", "npc"}
    );

    // Assert
    ASSERT_EQ(0, data.size());
}

TEST_F(JsonDatabaseTests, RetrieveRows_ColumnMatch_Valid_Column) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "npc";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    const auto data = db.RetrieveRows(
        "quests",
        rowSelector,
        {"quest", "npc"}
    );

    // Assert
    ASSERT_EQ(2, data.size());
    std::unordered_set< intmax_t > questsNotFound = { 42, 43 };
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ("quest", data[i][0].name);
        EXPECT_EQ(DataType::Integer, data[i][0].value.type);
        const auto quest = data[i][0].value.data.integer;
        auto questNotFound = questsNotFound.find(quest);
        EXPECT_FALSE(questNotFound == questsNotFound.end());
        (void)questsNotFound.erase(questNotFound);
        EXPECT_EQ("npc", data[i][1].name);
        EXPECT_EQ(DataType::Integer, data[i][1].value.type);
        EXPECT_EQ(1, data[i][1].value.data.integer);
    }
    EXPECT_TRUE(questsNotFound.empty());
}

TEST_F(JsonDatabaseTests, RetrieveRows_ColumnMatch_No_Such_Column) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "foobar";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    const auto data = db.RetrieveRows(
        "quests",
        rowSelector,
        {"quest", "npc"}
    );

    // Assert
    ASSERT_EQ(0, data.size());
}

TEST_F(JsonDatabaseTests, RetrieveRows_ColumnMatch_Wrong_Type_For_Key) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "key";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    const auto data = db.RetrieveRows(
        "kv",
        rowSelector,
        {"key", "value"}
    );

    // Assert
    ASSERT_EQ(0, data.size());
}

TEST_F(JsonDatabaseTests, RetrieveRows_All) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::All;
    const auto data = db.RetrieveRows(
        "quests",
        rowSelector,
        {"quest", "npc"}
    );

    // Assert
    ASSERT_EQ(3, data.size());
    std::set< std::pair< intmax_t, intmax_t > > entriesNotFound = {
        {1, 42},
        {1, 43},
        {2, 43},
    };
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ("quest", data[i][0].name);
        EXPECT_EQ(DataType::Integer, data[i][0].value.type);
        EXPECT_EQ("npc", data[i][1].name);
        EXPECT_EQ(DataType::Integer, data[i][1].value.type);
        const std::pair< intmax_t, intmax_t > entry = {
            data[i][1].value.data.integer,
            data[i][0].value.data.integer
        };
        auto entryNotFound = entriesNotFound.find(entry);
        EXPECT_FALSE(entryNotFound == entriesNotFound.end());
        (void)entriesNotFound.erase(entryNotFound);
    }
    EXPECT_TRUE(entriesNotFound.empty());
}

TEST_F(JsonDatabaseTests, UpdateRows_No_Such_Table) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::All;
    Value name;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    const auto numRowsUpdated = db.UpdateRows(
        "foobar",
        rowSelector,
        {
            {"quest", std::move(name)}
        }
    );

    // Assert
    EXPECT_EQ(0, numRowsUpdated);
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, UpdateRows_ColumnMatch_Valid_Column) {
    // Arrange
    auto expectedSerialization = startingSerialization;
    expectedSerialization["npcs"][0]["name"] = "Steve";

    // Act
    //
    // By the way, also include in the column descriptors a column which
    // doesn't actually exist.  We expect that this column's data is simply
    // ignored.
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "entity";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    Value name, foobar;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    foobar.data.integer = 123;
    foobar.type = DataType::Integer;
    const auto numRowsUpdated = db.UpdateRows(
        "npcs",
        rowSelector,
        {
            {"name", std::move(name)},
            {"foobar", std::move(foobar)},
        }
    );

    // Assert
    EXPECT_EQ(1, numRowsUpdated);
    VerifySerialization(expectedSerialization);
}

TEST_F(JsonDatabaseTests, UpdateRows_ColumnMatch_No_Such_Column) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "foobar";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    Value name;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    const auto numRowsUpdated = db.UpdateRows(
        "quests",
        rowSelector,
        {
            {"quest", std::move(name)},
        }
    );

    // Assert
    EXPECT_EQ(0, numRowsUpdated);
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, UpdateRows_ColumnMatch_Wrong_Type_For_Key) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "key";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    Value name;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    const auto numRowsUpdated = db.UpdateRows(
        "kv",
        rowSelector,
        {
            {"key", std::move(name)}
        }
    );

    // Assert
    EXPECT_EQ(0, numRowsUpdated);
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, UpdateRows_All) {
    // Arrange
    auto expectedSerialization = startingSerialization;
    expectedSerialization["npcs"][0]["name"] = "Steve";
    expectedSerialization["npcs"][1]["name"] = "Steve";

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::All;
    Value name;
    std::string nameStorage = "Steve";
    name.data.text = &nameStorage;
    name.type = DataType::Text;
    const auto numRowsUpdated = db.UpdateRows(
        "npcs",
        rowSelector,
        {
            {"name", std::move(name)},
        }
    );

    // Assert
    EXPECT_EQ(2, numRowsUpdated);
    VerifySerialization(expectedSerialization);
}

TEST_F(JsonDatabaseTests, DestroyRows_No_Such_Table) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::All;
    db.DestroyRows(
        "foobar",
        rowSelector
    );

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, DestroyRows_ColumnMatch_Valid_Column) {
    // Arrange
    auto expectedSerialization = startingSerialization;
    expectedSerialization["quests"].Remove(0);
    expectedSerialization["quests"].Remove(0);

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "npc";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    db.DestroyRows(
        "quests",
        rowSelector
    );

    // Assert
    VerifySerialization(expectedSerialization);
}

TEST_F(JsonDatabaseTests, DestroyRows_ColumnMatch_No_Such_Column) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "foobar";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    db.DestroyRows(
        "quests",
        rowSelector
    );

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, DestroyRows_ColumnMatch_Wrong_Type_For_Key) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::ColumnMatch;
    std::string columnNameStorage = "key";
    rowSelector.data.columnMatch.columnName = &columnNameStorage;
    rowSelector.data.columnMatch.value.type = DataType::Integer;
    rowSelector.data.columnMatch.value.data.integer = 1;
    db.DestroyRows(
        "kv",
        rowSelector
    );

    // Assert
    VerifyNoChanges();
}

TEST_F(JsonDatabaseTests, DestroyRows_All) {
    // Arrange

    // Act
    RowSelector rowSelector;
    rowSelector.type = RowSelectorType::All;
    db.DestroyRows(
        "quests",
        rowSelector
    );

    // Assert
}

TEST_F(JsonDatabaseTests, CreateSnapshot) {
    // Arrange
    Json::EncodingOptions encodingOptions;
    encodingOptions.reencode = true;
    const auto serializationAsString = startingSerialization.ToEncoding(
        encodingOptions
    );

    // Act
    const auto snapshot = db.CreateSnapshot();

    // Assert
    EXPECT_EQ(
        Blob(
            serializationAsString.begin(),
            serializationAsString.end()
        ),
        snapshot
    );
}

TEST_F(JsonDatabaseTests, InstallSnapshot) {
    // Arrange
    db = JsonDatabase();
    store->Destroy();
    (void)db.Open(store);
    const auto serializationAsString = startingSerialization.ToEncoding();
    const Blob snapshot{
        serializationAsString.begin(),
        serializationAsString.end()
    };

    // Act
    db.InstallSnapshot(snapshot);

    // Assert
    VerifyNoChanges();
}
