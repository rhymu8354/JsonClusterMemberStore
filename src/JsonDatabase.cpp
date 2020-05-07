/**
 * @file JsonDatabase.cpp
 *
 * This file contains the implementation of the
 * JsonClusterMemberStore::JsonDatabase class.
 */

#include <functional>
#include <Json/Value.hpp>
#include <JsonClusterMemberStore/JsonDatabase.hpp>
#include <memory>
#include <SystemAbstractions/IFile.hpp>
#include <SystemAbstractions/IFileSystemEntry.hpp>
#include <unordered_set>

namespace {

    using namespace ClusterMemberStore;

    const ColumnDefinition* GetColumnDefinition(
        const TableDefinition& tableDefinition,
        const std::string columnName
    ) {
        for (const auto& columnDefinition: tableDefinition.columnDefinitions) {
            if (columnDefinition.name == columnName) {
                return &columnDefinition;
            }
        }
        return nullptr;
    }

    Value DecodeValue(const Json::Value& value) {
        Value decodedValue;
        switch(value.GetType()) {
            case Json::Value::Type::String: {
                decodedValue.type = DataType::Text;
                // BUG: leaks the string.
                decodedValue.data.text = new std::string(value);
            } break;

            case Json::Value::Type::Integer: {
                decodedValue.type = DataType::Integer;
                decodedValue.data.integer = value;
            } break;

            case Json::Value::Type::FloatingPoint: {
                decodedValue.type = DataType::Real;
                decodedValue.data.real = value;
            } break;

            case Json::Value::Type::Boolean: {
                decodedValue.type = DataType::Boolean;
                decodedValue.data.boolean = value;
            } break;

            default: break;
        }
        return decodedValue;
    }

    Json::Value EncodeValue(const Value& value) {
        switch(value.type) {
            case DataType::Text:
                return *value.data.text;

            case DataType::Integer:
                return value.data.integer;

            case DataType::Real:
                return value.data.real;

            case DataType::Boolean:
                return value.data.boolean;

            default:
                return Json::Value();
        }
    }

    Json::Value EncodeRow(
        const TableDefinition& tableDefinition,
        const ColumnDescriptors& columns,
        size_t keyIndex
    ) {
        const auto numColumns = columns.size();
        auto row = Json::Object({});
        for (size_t i = 0; i < numColumns; ++i) {
            if (i == keyIndex) {
                continue;
            }
            const auto columnDefinition = GetColumnDefinition(
                tableDefinition,
                columns[i].name
            );
            if (columnDefinition == nullptr) {
                continue;
            }
            auto encodedValue = EncodeValue(columns[i].value);
            if (encodedValue.GetType() == Json::Value::Type::Invalid) {
                return Json::Value();
            } else {
                row[columns[i].name] = std::move(encodedValue);
            }
        }
        return row;
    }

}

namespace ClusterMemberStore {

    // Here we implement what we specified we would have in our interface.
    // This contains our private properties.
    struct JsonDatabase::Impl {
        // Types

        using TableVisitor = std::function<
            void(
                const std::string& key,
                Json::Value& row
            )
        >;

        // Properties

        std::shared_ptr< SystemAbstractions::IFileSystemEntry > store;
        TableDefinitions tables;
        Json::Value data = Json::Object({});

        // Methods

        bool IsStringType(const std::string& type) {
            return (type == "text");
        }

        const ColumnDefinition* FindKey(const TableDefinition& tableDefinition) {
            for (const auto& columnDefinition: tableDefinition.columnDefinitions) {
                if (columnDefinition.isKey) {
                    return &columnDefinition;
                }
            }
            return nullptr;
        }

        void WithTable(
            const std::string& tableName,
            TableVisitor visitor
        ) {
            const auto key = FindKey(tables[tableName]);
            auto& table = data[tableName];
            if (
                key
                && IsStringType(key->type)
            ) {
                for (const auto& key: table.GetKeys()) {
                    visitor(key, table[key]);
                }
            } else {
                for (size_t i = 0; i < table.GetSize(); ++i) {
                    auto& row = table[i];
                    visitor(
                        (
                            key
                            ? row[key->name]
                            : ""
                        ),
                        row
                    );
                }
            }
        }

        /**
         * Read and return the entire contents of the store.
         *
         * @param[out] value
         *     This is where to put the contents of the store.
         *
         * @return
         *     An indication of whether or not the function succeeded
         *     is returned.
         */
        bool ReadFromStore(Json::Value& value) {
            if (!store->OpenReadOnly()) {
                return false;
            }
            SystemAbstractions::IFile::Buffer buffer(store->GetSize());
            const auto amountRead = store->Read(buffer);
            store->Close();
            if (amountRead != buffer.size()) {
                return false;
            }
            value = Json::Value::FromEncoding(
                std::string(
                    buffer.begin(),
                    buffer.end()
                )
            );
            return (value.GetType() != Json::Value::Type::Invalid);
        }

        /**
         * Encode the given JSON value and write it out to the store.
         *
         * @param[in] value
         *     This is the JSON value to encode and write to the store.
         *
         * @return
         *     An indication of whether or not the function succeeded
         *     is returned.
         */
        bool WriteToStore(const Json::Value& value) {
            if (!store->OpenReadWrite()) {
                return false;
            }
            Json::EncodingOptions jsonEncodingOptions;
            jsonEncodingOptions.pretty = true;
            jsonEncodingOptions.reencode = true;
            const auto encoding = value.ToEncoding(jsonEncodingOptions);
            SystemAbstractions::IFile::Buffer buffer(
                encoding.begin(),
                encoding.end()
            );
            if (store->Write(buffer) != buffer.size()) {
                return false;
            }
            if (!store->SetSize(buffer.size())) {
                return false;
            }
            return true;
        }

        bool Deserialize(Json::Value&& serialization) {
            data = std::move(serialization);
            const auto& schema = data["schema"];
            if (schema.GetType() != Json::Value::Type::Object) {
                return false;
            }
            for (const auto keyValue: schema) {
                const auto& tableName = keyValue.key();
                const auto& tableSchema = keyValue.value();
                if (tableSchema.GetType() != Json::Value::Type::Array) {
                    return false;
                }
                TableDefinition tableDefinition;
                std::unordered_set< std::string > columnNames;
                for (size_t i = 0; i < tableSchema.GetSize(); ++i) {
                    const auto& column = tableSchema[i];
                    if (column.GetType() != Json::Value::Type::Object) {
                        return false;
                    }
                    const auto columnName = (std::string)column["name"];
                    const auto columnType = (std::string)column["type"];
                    const auto columnIsKey = (bool)column["key"];
                    if (
                        columnName.empty()
                        || (columnNames.find(columnName) != columnNames.end())
                    ) {
                        return false;
                    }
                    (void)columnNames.insert(columnName);
                    tableDefinition.columnDefinitions.push_back({
                        columnName, columnType, columnIsKey
                    });
                }
                tables[tableName] = std::move(tableDefinition);
            }
            return true;
        }

        Json::Value Serialize() {
            auto serialization = data;
            serialization["schema"] = Json::Object({});
            auto& schema = serialization["schema"];
            for (const auto& table: tables) {
                const auto& tableName = table.first;
                const auto& tableDefinition = table.second;
                schema[tableName] = Json::Array({});
                auto& tableSchema = schema[tableName];
                for (const auto& column: tableDefinition.columnDefinitions) {
                    tableSchema.Add(Json::Object({
                        {"name", column.name},
                        {"type", column.type},
                        {"key", column.isKey},
                    }));
                }
            }
            return serialization;
        }
    };

    JsonDatabase::~JsonDatabase() noexcept = default;
    JsonDatabase::JsonDatabase(JsonDatabase&&) noexcept = default;
    JsonDatabase& JsonDatabase::operator=(JsonDatabase&&) noexcept = default;

    JsonDatabase::JsonDatabase()
        : impl_(new Impl())
    {
    }

    bool JsonDatabase::Open(std::shared_ptr< SystemAbstractions::IFileSystemEntry > store) {
        impl_->store = store;
        Json::Value serialization;
        if (!impl_->ReadFromStore(serialization)) {
            return false;
        }
        return impl_->Deserialize(std::move(serialization));
    }

    bool JsonDatabase::Commit() {
        if (!impl_->store) {
            return false;
        }
        return impl_->WriteToStore(
            impl_->Serialize()
        );
    }

    void JsonDatabase::CreateTable(
        const std::string& tableName,
        const TableDefinition& tableDefinition
    ) {
        impl_->tables[tableName] = tableDefinition;
        const auto key = impl_->FindKey(tableDefinition);
        if (
            key
            && impl_->IsStringType(key->type)
        ) {
            impl_->data[tableName] = Json::Object({});
        } else {
            impl_->data[tableName] = Json::Array({});
        }
    }

    TableDefinitions JsonDatabase::DescribeTables() {
        return impl_->tables;
    }

    void JsonDatabase::RenameTable(
        const std::string& oldTableName,
        const std::string& newTableName
    ) {
        if (newTableName.empty()) {
            return; // new table name empty
        }
        if (impl_->tables.find(newTableName) != impl_->tables.end()) {
            return; // new table name taken
        }
        auto tablesEntry = impl_->tables.find(oldTableName);
        if (tablesEntry == impl_->tables.end()) {
            return; // no such table
        }
        impl_->tables[newTableName] = std::move(tablesEntry->second);
        (void)impl_->tables.erase(tablesEntry);
        impl_->data[newTableName] = std::move(impl_->data[oldTableName]);
        impl_->data.Remove(oldTableName);
    }

    void JsonDatabase::AddColumn(
        const std::string& tableName,
        const ColumnDefinition& columnDefinition
    ) {
        auto tablesEntry = impl_->tables.find(tableName);
        if (tablesEntry == impl_->tables.end()) {
            return; // no such table
        }
        auto& tableDefinition = tablesEntry->second;
        tableDefinition.columnDefinitions.push_back(columnDefinition);
    }

    void JsonDatabase::DestroyColumn(
        const std::string& tableName,
        const std::string& columnName
    ) {
        auto tablesEntry = impl_->tables.find(tableName);
        if (tablesEntry == impl_->tables.end()) {
            return; // no such table
        }
        auto& tableDefinition = tablesEntry->second;
        auto& columns = tableDefinition.columnDefinitions;
        for (
            auto columnsEntry = columns.begin();
            columnsEntry != columns.end();
            ++columnsEntry
        ) {
            const auto& columnDefinition = *columnsEntry;
            if (columnDefinition.name == columnName) {
                (void)columns.erase(columnsEntry);
                impl_->WithTable(
                    tableName,
                    [columnName](const std::string& /*key*/, Json::Value& row){
                        row.Remove(columnName);
                    }
                );
                break;
            }
        }
    }

    void JsonDatabase::DestroyTable(
        const std::string& tableName
    ) {
        (void)impl_->tables.erase(tableName);
        (void)impl_->data.Remove(tableName);
    }

    void JsonDatabase::CreateRow(
        const std::string& tableName,
        const ColumnDescriptors& columns
    ) {
        auto tablesEntry = impl_->tables.find(tableName);
        if (tablesEntry == impl_->tables.end()) {
            return; // no such table
        }
        const auto& tableSchema = tablesEntry->second;
        auto& table = impl_->data[tableName];
        const auto numColumns = columns.size();
        const auto key = impl_->FindKey(tableSchema);
        if (key) {
            // Locate the key in the row to be inserted.
            size_t keyIndex = numColumns;
            for (size_t i = 0; i < numColumns; ++i) {
                if (columns[i].name == key->name) {
                    keyIndex = i;
                    break;
                }
            }
            if (keyIndex == numColumns) {
                return; // key not provided in the data for the row
            }

            // Encode the key.
            auto encodedKey = EncodeValue(columns[keyIndex].value);
            if (encodedKey.GetType() == Json::Value::Type::Invalid) {
                return;
            }

            // If the keys for this table are strings, we're going to
            // be adding the row as a value in a JSON object.
            if (impl_->IsStringType(key->type)) {
                if (encodedKey.GetType() != Json::Value::Type::String) {
                    return; // key must be a string
                }
                const auto& key = *columns[keyIndex].value.data.text;
                if (table.Has(key)) {
                    return; // table already has row with this key
                }
                auto row = EncodeRow(tableSchema, columns, keyIndex);
                if (row.GetType() == Json::Value::Type::Invalid) {
                    return;
                }
                table[key] = std::move(row);
                return;
            }

            // Otherwise, make sure the table doesn't already have a
            // row with this key.
            else {
                for (size_t i = 0; i < table.GetSize(); ++i) {
                    if (table[i][key->name] == encodedKey) {
                        return;
                    }
                }
            }
        }

        // If we reach here, the table either has no key, or the keys are
        // non-strings, so we're going to be adding the row as a value in a
        // JSON array, where the key (if any) is stored in the array.
        auto row = EncodeRow(tableSchema, columns, numColumns);
        if (row.GetType() == Json::Value::Type::Invalid) {
            return;
        }
        table.Add(std::move(row));
    }

    DataSet JsonDatabase::RetrieveRows(
        const std::string& tableName,
        const RowSelector& rowSelector,
        const ColumnSelector& columnSelector
    ) {
        // Verify table exists.
        auto tablesEntry = impl_->tables.find(tableName);
        if (tablesEntry == impl_->tables.end()) {
            return {}; // no such table
        }

        // Determine if the table is using a string key.
        const auto& tableSchema = tablesEntry->second;
        const auto keySchema = impl_->FindKey(tableSchema);
        const auto keyIsString = (
            keySchema
            && impl_->IsStringType(keySchema->type)
        );

        // Make a function we will use to retrieve a column for a row
        // as a JSON value.
        const auto getColumnAsJson = [&](
            const std::string& columnName,
            const Json::Value& row,
            const std::string& key
        ){
            // If the key is requested and it's a string,
            // we have it handed to us already.
            if (
                keyIsString
                && (columnName == keySchema->name)
            ) {
                return Json::Value(key);
            }

            // Otherwise, fetch the column (if it exists) from the row.
            else if (row.Has(columnName)) {
                return row[columnName];
            }

            // If we reach here, there is no such column in the row.
            return Json::Value();
        };

        // Make a function we will use to decode some
        // column of table row.
        const auto decodeColumn = [&](
            const std::string& columnName,
            const Json::Value& row,
            const std::string& key
        ){
            const auto columnAsJson = getColumnAsJson(columnName, row, key);
            return DecodeValue(columnAsJson);
        };

        // Make a delegate which performs the requested filtering.
        using RowSelectorDelegate = std::function<
            bool(
                const std::string& key,
                Json::Value& row
            )
        >;
        RowSelectorDelegate rowSelectorDelegate;
        switch (rowSelector.type) {
            case RowSelectorType::All: {
                rowSelectorDelegate = [](
                    const std::string& key,
                    Json::Value& row
                ){
                    return true;
                };
            } break;

            case RowSelectorType::ColumnMatch: {
                const auto columnValue = EncodeValue(
                    rowSelector.data.columnMatch.value
                );
                rowSelectorDelegate = [
                    columnValue,
                    getColumnAsJson,
                    &rowSelector
                ](
                    const std::string& key,
                    Json::Value& row
                ){
                    return (
                        getColumnAsJson(*rowSelector.data.columnMatch.columnName, row, key)
                        == EncodeValue(rowSelector.data.columnMatch.value)
                    );
                };
            } break;

            default: return {};
        }

        // Visit the table, collecting requested columns from rows which meet
        // the filter requirements.
        DataSet rows;
        impl_->WithTable(
            tableName,
            [&](
                const std::string& key,
                Json::Value& row
            ){
                // Skip this row if it doesn't meet the filter requirements.
                if (!rowSelectorDelegate(key, row)) {
                    return;
                }

                // Pick out the columns requested for the row.
                ColumnDescriptors columns;
                for (const auto& columnName: columnSelector) {
                    // Add the column with its name to the results.
                    //
                    // TODO: consider just returning the value without the
                    // name.
                    ColumnDescriptor column;
                    column.name = columnName;
                    column.value = decodeColumn(columnName, row, key);
                    columns.push_back(std::move(column));
                }

                // Add the row to the output data set.
                rows.push_back(std::move(columns));
            }
        );
        return rows;
    }

    size_t JsonDatabase::UpdateRows(
        const std::string& tableName,
        const RowSelector& rowSelector,
        const ColumnDescriptors& columns
    ) {
        // Verify table exists.
        auto tablesEntry = impl_->tables.find(tableName);
        if (tablesEntry == impl_->tables.end()) {
            return {}; // no such table
        }

        // Determine if the table is using a string key.
        const auto& tableSchema = tablesEntry->second;
        const auto keySchema = impl_->FindKey(tableSchema);
        const auto keyIsString = (
            keySchema
            && impl_->IsStringType(keySchema->type)
        );

        // Make a function we will use to retrieve a column for a row
        // as a JSON value.
        const auto getColumnAsJson = [&](
            const std::string& columnName,
            const Json::Value& row,
            const std::string& key
        ){
            // If the key is requested and it's a string,
            // we have it handed to us already.
            if (
                keyIsString
                && (columnName == keySchema->name)
            ) {
                return Json::Value(key);
            }

            // Otherwise, fetch the column (if it exists) from the row.
            else if (row.Has(columnName)) {
                return row[columnName];
            }

            // If we reach here, there is no such column in the row.
            return Json::Value();
        };

        // Make a function we will use to decode some
        // column of table row.
        const auto decodeColumn = [&](
            const std::string& columnName,
            const Json::Value& row,
            const std::string& key
        ){
            const auto columnAsJson = getColumnAsJson(columnName, row, key);
            return DecodeValue(columnAsJson);
        };

        // Make a delegate which performs the requested filtering.
        using RowSelectorDelegate = std::function<
            bool(
                const std::string& key,
                Json::Value& row
            )
        >;
        RowSelectorDelegate rowSelectorDelegate;
        switch (rowSelector.type) {
            case RowSelectorType::All: {
                rowSelectorDelegate = [](
                    const std::string& key,
                    Json::Value& row
                ){
                    return true;
                };
            } break;

            case RowSelectorType::ColumnMatch: {
                const auto columnValue = EncodeValue(
                    rowSelector.data.columnMatch.value
                );
                rowSelectorDelegate = [
                    columnValue,
                    getColumnAsJson,
                    &rowSelector
                ](
                    const std::string& key,
                    Json::Value& row
                ){
                    return (
                        getColumnAsJson(*rowSelector.data.columnMatch.columnName, row, key)
                        == EncodeValue(rowSelector.data.columnMatch.value)
                    );
                };
            } break;

            default: return {};
        }

        // Visit the table, updated requested columns in rows which meet
        // the filter requirements.
        size_t numRowsUpdated = 0;
        impl_->WithTable(
            tableName,
            [&](
                const std::string& key,
                Json::Value& row
            ){
                // Skip this row if it doesn't meet the filter requirements.
                if (!rowSelectorDelegate(key, row)) {
                    return;
                }

                // Update the columns requested in the row.
                bool rowUpdated = false;
                for (const auto& columnDescriptor: columns) {
                    const auto columnDefinition = GetColumnDefinition(
                        tableSchema,
                        columnDescriptor.name
                    );
                    if (columnDefinition == nullptr) {
                        continue; // no such column
                    }
                    auto encodedValue = EncodeValue(columnDescriptor.value);
                    if (encodedValue.GetType() == Json::Value::Type::Invalid) {
                        continue; // cannot represent data in JSON
                    } else {
                        row[columnDescriptor.name] = std::move(encodedValue);
                        rowUpdated = true;
                    }
                }
                if (rowUpdated) {
                    ++numRowsUpdated;
                }
            }
        );
        return numRowsUpdated;
    }

    size_t JsonDatabase::DestroyRows(
        const std::string& tableName,
        const RowSelector& rowSelector
    ) {
        // Verify table exists.
        auto tablesEntry = impl_->tables.find(tableName);
        if (tablesEntry == impl_->tables.end()) {
            return 0; // no such table
        }

        // Determine if the table is using a string key.
        const auto& tableSchema = tablesEntry->second;
        const auto keySchema = impl_->FindKey(tableSchema);
        const auto keyIsString = (
            keySchema
            && impl_->IsStringType(keySchema->type)
        );

        // Make a function we will use to retrieve a column for a row
        // as a JSON value.
        const auto getColumnAsJson = [&](
            const std::string& columnName,
            const Json::Value& row,
            const std::string& key
        ){
            // If the key is requested and it's a string,
            // we have it handed to us already.
            if (
                keyIsString
                && (columnName == keySchema->name)
            ) {
                return Json::Value(key);
            }

            // Otherwise, fetch the column (if it exists) from the row.
            else if (row.Has(columnName)) {
                return row[columnName];
            }

            // If we reach here, there is no such column in the row.
            return Json::Value();
        };

        // Make a delegate which performs the requested filtering.
        using RowSelectorDelegate = std::function<
            bool(
                const std::string& key,
                Json::Value& row
            )
        >;
        RowSelectorDelegate rowSelectorDelegate;
        switch (rowSelector.type) {
            case RowSelectorType::All: {
                rowSelectorDelegate = [](
                    const std::string& key,
                    Json::Value& row
                ){
                    return true;
                };
            } break;

            case RowSelectorType::ColumnMatch: {
                const auto columnValue = EncodeValue(
                    rowSelector.data.columnMatch.value
                );
                rowSelectorDelegate = [
                    columnValue,
                    getColumnAsJson,
                    &rowSelector
                ](
                    const std::string& key,
                    Json::Value& row
                ){
                    return (
                        getColumnAsJson(*rowSelector.data.columnMatch.columnName, row, key)
                        == EncodeValue(rowSelector.data.columnMatch.value)
                    );
                };
            } break;

            default: return 0;
        }

        // Enumerate the table, dropping rows which meet the filter
        // requirements.  Count the number of rows deleted.
        const auto key = impl_->FindKey(impl_->tables[tableName]);
        auto& table = impl_->data[tableName];
        size_t numRowsDeleted = 0;
        if (
            key
            && impl_->IsStringType(key->type)
        ) {
            for (const auto& key: table.GetKeys()) {
                if (rowSelectorDelegate(key, table[key])) {
                    table.Remove(key);
                    ++numRowsDeleted;
                }
            }
        } else {
            size_t i = 0;
            while (i < table.GetSize()) {
                auto& row = table[i];
                if (
                    rowSelectorDelegate(
                        (
                            key
                            ? row[key->name]
                            : ""
                        ),
                        table[i]
                    )
                ) {
                    table.Remove(i);
                    ++numRowsDeleted;
                } else {
                    ++i;
                }
            }
        }
        return numRowsDeleted;
    }

    Blob JsonDatabase::CreateSnapshot() {
        const auto serialization = impl_->Serialize().ToEncoding();
        return Blob(
            serialization.begin(),
            serialization.end()
        );
    }

    void JsonDatabase::InstallSnapshot(const Blob& blob) {
        auto serialization = Json::Value::FromEncoding(
            std::string(
                blob.begin(),
                blob.end()
            )
        );
        (void)impl_->Deserialize(std::move(serialization));
    }

}
