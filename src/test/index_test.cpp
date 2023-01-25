#include "../index.h"

#include <iostream>

void insertTest(const int N) {
	std::cout << "insertion test\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	for (int i = 0; i <N; i++) {
		std::vector<String> data;
		for (int j = 0; j < 2; j++)
			data.push_back(std::to_string(rand()));
		auto key = PackedData(types, data);
		tree.insert(key, i+1);
		//tree.dump();
		tree.checkIntegrity();
	}
}

void removeTest(const int N) {
	std::cout << "sequential insertions then removals test\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	tree.dump();
	std::vector<std::vector<String>> data(N);
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < 2; j++)
			data[i].push_back(std::to_string(rand()));
	}

	for (int i = 0; i < N; i++) {
		auto key = PackedData(types, data[i]);
		tree.insert(key, i+1);
		tree.checkIntegrity();
	}
	//tree.dump();
	for (int i = 0; i < N; i++) {
		auto key = PackedData(types, data[i]);
		tree.remove(key, i+1);
		//tree.dump();
		tree.checkIntegrity();
	}
	tree.dump();
}

void mixedTest(const int N) {
	std::cout << "mixed insertions and removals test\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	if(N < 100)
		tree.dump();
	std::vector<std::vector<String>> data(N);
	std::vector<int> isUsed(N);
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < 2; j++)
			data[i].push_back(std::to_string(rand()));
	}

	for (int i = 0; i < N*3; i++) {
		int index = rand() % N;
		if (!isUsed[index]) {
			isUsed[index] = true;
			auto key = PackedData(types, data[index]);
			tree.insert(key, index + 1);
			//tree.dump();
			tree.checkIntegrity();
		}
		else {
			isUsed[index] = false;
			auto key = PackedData(types, data[index]);
			tree.remove(key, index + 1);
			//tree.dump();
			tree.checkIntegrity();
		}
	}
	if(N < 100)
		tree.dump();
}

int main() {
	insertTest(1000);
	removeTest(1);
	removeTest(5);
	removeTest(12);
	removeTest(16);
	removeTest(20);
	removeTest(100);
	removeTest(1000);
	mixedTest(5);
	mixedTest(20);
	mixedTest(50);
	mixedTest(100);
	mixedTest(300);
	mixedTest(1000);
}