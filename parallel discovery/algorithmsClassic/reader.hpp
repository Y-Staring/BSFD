#ifndef READER_H
#define READER_H

#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#define MAX_SIZE 80
#define MIN_COMPARE 3
#define MIN_MATRIX 2


class Reader {
public:
  std::vector<std::vector<int>> data;
  std::vector<std::vector<std::string>> dataset;
  std::vector<std::unordered_map<std::string, int>> value_map;
  std::vector<std::string> attributes;


  int nrow;
  int ncol;

//预先分配存储空间
  Reader(std::string path) {
    data.reserve(1000000);
    dataset.reserve(70);
  }


  void read_data_a(std::string path) {
      std::ifstream in(path);
      // handle column names
      std::string line;
      std::getline(in, line);
      int count = 0;
      int countk = MAX_SIZE;
      std::stringstream columns(line);
      std::string attribute;
      std::vector<int> tmp;
      std::vector<int> tmp_v;
      std::vector<std::vector<int>> tmp_m;
      while (std::getline(columns, attribute, ','))
      {
          attributes.push_back(attribute);
      }
      ncol = attributes.size();
    
      for (std::string line; std::getline(in, line); ) {
          std::vector<std::string> row;
          std::stringstream ss(line);
          std::string cell;
          while (std::getline(ss, cell, ',')) {    
              row.push_back(cell);
          }
          if (count < MAX_SIZE) {
              count = count + 1;
              dataset.push_back(row);
          }
          else {
              for (int i = 0; i < MAX_SIZE; i++)
              {
                  int match = 0;
                  for (int j = 0; j < ncol; j++) {
                      if (dataset[i][j] == row[j]) {
                          tmp_v.push_back(1);
                          match = match + 1;
                      }
                      else {
                          tmp_v.push_back(0);
                      }
                  }
                  if (match >= MIN_COMPARE) {
                      tmp_m.push_back(tmp_v);
                  }
                  tmp_v.clear();
              }
              if (tmp_m.size() >= MIN_MATRIX) {
                  countk = countk + 1;
                  for (int k = 0; k < tmp_m.size(); k++) {
                      data.push_back(tmp_m[k]);
                  }
                  int rand_int = (rand() % (countk - 1)) + 1;
                  if (rand_int < MAX_SIZE) {
                      for (int j = 0; j < ncol; j++) {
                          dataset[rand_int - 1][j] = row[j];
                      }
                  }
              }
              tmp_m.clear();
          }
      }
      nrow = countk;
      std::cout << "size" << data.size() << std::endl;
  }


  void parse_line_compare(std::string& line, std::vector<std::vector<int>>& data, std::vector<std::vector<int>>& matrix, int count) {
      int left = 0;
      int right = 0;
      int col_idx = 0;
      std::vector<int> tmp;
      std::vector<int> tmp_v;
      bool flage = false;
      while (right < line.length()) {
          if (line[right] == ',' && line[right + 1] != ' ') {
              // a new column
              auto value = line.substr(left, right - left);
              auto code = hash(value, col_idx);
              tmp.push_back(code);
              ++col_idx;
              left = right + 1;
          }
          ++right;
      }
      auto value = line.substr(left, right - left);
      auto code = hash(value, col_idx);
      tmp.push_back(code);
      for (int i = 0; i < MAX_SIZE; i++)
      {
          int match = 0;
          for (int j = 0; j < tmp.size(); j++) {
              if (data[i][j] == tmp[j]) {
                  tmp_v.push_back(1);
                  match = match + 1;
              }
              else {
                  tmp_v.push_back(0);
              }
          }
          if (match >= MIN_COMPARE) {
              matrix.push_back(tmp_v);
              flage = true;
          }
          tmp_v.clear();
      }
      if (flage) {
          int rand_int = (rand() % (count - 1)) + 1;
          if (rand_int <= MAX_SIZE) {
              data[rand_int - 1] = tmp;
          }
      }
  }


  void read_data(std::string path) {
    std::ifstream in(path);

    // handle column names
    std::string line;
    std::getline(in, line);
    
    std::stringstream columns(line);
    std::string attribute;
    while(std::getline(columns, attribute, ','))
    {
        attributes.push_back(attribute);
    }

    for (std::string line; std::getline(in, line); ) {

      data.emplace_back();
      if (data.size() >= 1) {
        data[data.size() - 1].reserve(data[0].size());
      }

      parse_line(line, data[data.size() - 1]);
    }
    nrow = data.size();
    ncol = value_map.size();
  }

  void parse_line(std::string& line, std::vector<int>& row) {
    int left = 0;
    int right = 0;
    int col_idx = 0;
    while (right < line.length()) {
      if (line[right] == ',' && line[right + 1] != ' ') {
        // a new column
        auto value = line.substr(left, right - left);
        auto code = hash(value, col_idx);         //生成一个哈希码
        row.push_back(code);
        ++col_idx;
        left = right + 1;
      }
      ++right;
    }

    auto value = line.substr(left, right - left);
    auto code = hash(value, col_idx);    //生成hash code
    row.push_back(code);
  }

  int hash(std::string& value, int col_idx) {
    if (col_idx >= value_map.size()) {
      value_map.emplace_back(std::unordered_map<std::string, int>());
      value_map[value_map.size() - 1].reserve(100000);
    }
    auto iter = value_map[col_idx].find(value);
    if (iter != value_map[col_idx].end()) {
      return iter->second;
    } else {
      int code = value_map[col_idx].size();  //就是数值，这样每加一个元素,code+1，不会有重复
      value_map[col_idx][value] = code;
      return code;
    }
  }

  bool check_integrity() {
    bool flag = true;
    for (int i = 0; i < nrow; ++i) {
      if (data[i].size() != ncol) {
        std::cout << "Error!\n"
                  << "But row " << i << " has " << data[i].size() << " columns.\n";
        flag = false;
      }
    }
    std::cout << "Total Rows: " << nrow << "\n"
              << "Total Cols: " << ncol << "\n";
    return flag;
  }
};

#endif // READER_H
