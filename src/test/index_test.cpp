#include "../index.h"

#include <iostream>

void insertTest(const int N) {
	std::cout << "insert test\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	for (int i = 0; i < N; i++) {
		std::vector<String> data;
		for (int j = 0; j < 2; j++)
			data.push_back(std::to_string(rand()));
		auto key = PackedData(types, data);
		tree.insert(key, i);
		//tree.dump();
		tree.checkIntegrity();
	}
}

void removeTest(const int N) {
	std::cout << "insert then remove test\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	tree.dump();
	std::vector<std::vector<String>> data;
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < 2; j++)
			data[i].push_back(std::to_string(rand()));
	}

	for (int i = 0; i < N; i++) {
		auto key = PackedData(types, data[i]);
		tree.insert(key, i);
	}
	for (int i = 0; i < N; i++) {
		auto key = PackedData(types, data[i]);
		tree.remove(key, i);
		tree.checkIntegrity();
	}
	tree.dump();
}

int main() {
	insertTest(10000);
	//removeTest(1000);
}