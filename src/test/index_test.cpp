#include "../index.h"

#include <iostream>
#include <cassert>
#include <numeric>
#include <algorithm>

void insertTest(const int N) {
	std::cout << "insertion test: N = " << N << "\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	std::vector<std::vector<String>> data(N);
	std::vector<PackedData> packed(N);
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < 2; j++)
			data[i].push_back(std::to_string(rand()));
		packed[i] = PackedData(types, data[i]);
	}

	for (int i = 0; i < N; i++) {
		tree.insert(packed[i], i + 1);
		tree.checkIntegrity();
	}
}

void removeTest(const int N) {
	std::cout << "sequential insertions then removals test: N = " << N << "\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	tree.dump();
	std::vector<std::vector<String>> data(N);
	std::vector<PackedData> packed(N);
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < 2; j++)
			data[i].push_back(std::to_string(rand()));
		packed[i] = PackedData(types, data[i]);
	}

	for (int i = 0; i < N; i++) {
		tree.insert(packed[i], i + 1);
		tree.checkIntegrity();
	}
	for (int i = 0; i < N; i++) {
		tree.remove(packed[i], i + 1);
		tree.checkIntegrity();
	}
	tree.dump();
}

void mixedTest(const int N) {
	std::cout << "mixed insertions and removals test: N = " << N << "\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	std::vector<std::vector<String>> data(N);
	std::vector<PackedData> packed(N);
	std::vector<int> isUsed(N);
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < 2; j++)
			data[i].push_back(std::to_string(rand()));
		packed[i] = PackedData(types, data[i]);
	}

	auto insert = [&](int index) {
		if (!isUsed[index]) {
			isUsed[index] = true;
			tree.insert(packed[index], index + 1);
			tree.checkIntegrity();
		}
	};

	auto flip = [&](int index) {
		if (!isUsed[index]) {
			isUsed[index] = true;
			tree.insert(packed[index], index + 1);
			tree.checkIntegrity();
		}
		else {
			isUsed[index] = false;
			tree.remove(packed[index], index + 1);
			tree.checkIntegrity();
		}
	};

	for (int i = 0; i < N*3; i++) {
		int index = rand() % N;
		flip(index);
	}
}

void mixedTest2(const int N) {
	std::cout << "sequential insertions then mixed insertions and removals test: N = " << N << "\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	std::vector<std::vector<String>> data(N);
	std::vector<PackedData> packed(N);
	std::vector<int> isUsed(N);
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < 2; j++)
			data[i].push_back(std::to_string(rand()));
		packed[i] = PackedData(types, data[i]);
	}

	auto insert = [&](int index) {
		if (!isUsed[index]) {
			isUsed[index] = true;
			tree.insert(packed[index], index + 1);
			tree.checkIntegrity();
		}
	};

	auto flip = [&](int index) {
		if (!isUsed[index]) {
			isUsed[index] = true;
			tree.insert(packed[index], index + 1);
			tree.checkIntegrity();
		}
		else {
			isUsed[index] = false;
			tree.remove(packed[index], index + 1);
			tree.checkIntegrity();
		}
	};

	for (int i = 0; i < N; i++) {
		if (rand() % 2) {
			int index = rand() % N;
			insert(index);
		}
	}

	for (int i = 0; i < N * 2; i++) {
		int index = rand() % N;
		flip(index);
	}
}

void selectTest(const int N) {
	std::cout << "select test: N = " << N << "\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	std::vector<std::vector<String>> data(N);
	std::vector<PackedData> packed(N);
	std::vector<int> isUsed(N);
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < 2; j++)
			data[i].push_back(std::to_string(rand()));
		packed[i] = PackedData(types, data[i]);
	}

	auto insert = [&](int index) {
		if (!isUsed[index]) {
			isUsed[index] = true;
			tree.insert(packed[index], index + 1);
			//tree.checkIntegrity();
		}
	};

	auto flip = [&](int index) {
		if (!isUsed[index]) {
			isUsed[index] = true;
			tree.insert(packed[index], index + 1);
			//tree.checkIntegrity();
		}
		else {
			isUsed[index] = false;
			tree.remove(packed[index], index + 1);
			//tree.checkIntegrity();
		}
	};

	for (int i = 0; i < N; i++) {
		if (rand() % 2) {
			int index = rand() % N;
			insert(index);
		}
	}

	for (int i = 0; i < N * 2; i++) {
		int index = rand() % N;
		flip(index);
	}

	for (int i = 0; i < N; i++) {
		if (tree.select(packed[i], i + 1) != (isUsed[i] != 0)) {
			tree.dump();
			auto expected = (isUsed[i] != 0);
			auto res = tree.select(packed[i], i + 1);
		}
		assert(tree.select(packed[i], i + 1) == (isUsed[i] != 0));
	}
}

void rangeSelectTest(const int N) {
	std::cout << "range select test: N = " << N << "\n";
	std::vector<DataType> types = { DataType::INT64, DataType::INT32 };
	Index tree(types, { "NUMBER", "COLOR" }, true);

	std::vector<std::vector<int>> data(N);
	std::vector<PackedData> packed(N);
	std::vector<int> isUsed(N);
	for (int i = 0; i < N; i++) {
		std::vector<String> str(2);
		for (int j = 0; j < 2; j++) {
			data[i].push_back(rand());
			str[j] = std::to_string(data[i][j]);
		}
		packed[i] = PackedData(types, str);
	}

	auto insert = [&](int index) {
		if (!isUsed[index]) {
			isUsed[index] = true;
			tree.insert(packed[index], index + 1);
			//tree.checkIntegrity();
		}
	};

	auto flip = [&](int index) {
		if (!isUsed[index]) {
			isUsed[index] = true;
			tree.insert(packed[index], index + 1);
			//tree.checkIntegrity();
		}
		else {
			isUsed[index] = false;
			tree.remove(packed[index], index + 1);
			//tree.checkIntegrity();
		}
	};

	for (int i = 0; i < N; i++) {
		if (rand() % 2) {
			int index = rand() % N;
			insert(index);
		}
	}

	for (int i = 0; i < N * 2; i++) {
		int index = rand() % N;
		flip(index);
	}

	std::vector<int> indirect(N);
	std::iota(indirect.begin(), indirect.end(), 0);
	std::sort(indirect.begin(), indirect.end(), [&](int i, int j) {return data[i] < data[j]; });

	std::vector<int> inverted(N);
	for (int i = 0; i < N; i++)
		inverted[indirect[i]] = i;

	for(int len=0, loop=0; loop < N && len < N*10; loop++){
		int index1 = rand() % N;
		int index2 = rand() % N;
		if (data[index1] > data[index2])
			std::swap(index1, index2);
		auto res = tree.selectRange(packed[index1], packed[index2]);
		std::sort(res.begin(), res.end());
		std::vector<int> expected;
		for (int i = inverted[index1]; i <= inverted[index2]; i++)
			if(isUsed[indirect[i]])
				expected.push_back(indirect[i]+1);
		std::sort(expected.begin(), expected.end());
		assert(res == expected);
		len += res.size();
	}
}

int main() {
	std::vector<int> ns;
	for (int i = 1; i < 20; i++)
		ns.push_back(i);
	for (int i = 20; i < 100; i += 5)
		ns.push_back(i);
	for (int i = 100; i < 1000; i += 100)
		ns.push_back(i);
	for (int i = 1000; i <= 3000; i += 1000)
		ns.push_back(i);

	for (auto n : ns)
		insertTest(n);

	for (auto n : ns)
		removeTest(n);

	for (auto n : ns)
		mixedTest(n);

	for (auto n : ns)
		mixedTest2(n);

	for (auto n : ns)
		selectTest(n);

	for (auto n : ns)
		rangeSelectTest(n);
}