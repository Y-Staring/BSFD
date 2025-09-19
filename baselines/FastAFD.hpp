#pragma once
#ifndef FAST_AFD_H // (optional) keep if you prefer double guard; otherwise remove this + the #endif at bottom.
#define FAST_AFD_H

#include <iostream>
#include <queue>
#include <vector>
#include <string>
#include <stack>
#include <unordered_map>
#include <algorithm>
#include <set>
#include <cstdio>
#include <cassert>
#include <cmath>
#include <limits>
#include "reader.hpp"

class FastAFD {
public:
    // FastAFD with thresholds (support/confidence)
    FastAFD(std::string path, double support_thr = 0.10, double conf_thr = 0.90)
        : support_thr_(support_thr), conf_thr_(conf_thr) {
        auto r = Reader(path);
        r.read_data_a(path);
        data = std::move(r.data);
        nrow = data.size();
        nrowk = r.nrow;
        ncol = r.ncol;
        tabu_for_unique_cols = 0;
        tabu_for_unuse_cols = 0;
        NodeSet.resize(bitmap.at(ncol));
        set_part_map.reserve(1500);
        cov_order_map.reserve(1500);
        metrics_map_.reserve(2048);
    }

    // Compute covariance matrix among columns
    void compute_cov() {
        cov.resize(ncol);
        std::vector<double> nrow_avg(ncol, 0.0);
        for (int i = 0; i < ncol; i++) {
            double sum = 0.0;
            for (int j = 0; j < nrow; j++) sum += data[j][i];
            nrow_avg[i] = sum / (nrow ? nrow : 1);
        }
        for (int i = 0; i < ncol; i++) {
            cov[i].reserve(ncol);
            for (int k = 0; k < ncol; k++) {
                double s = 0.0;
                for (int j = 0; j < nrow; j++) {
                    s += (data[j][i] - nrow_avg[i]) * (data[j][k] - nrow_avg[k]);
                }
                cov[i].push_back(nrow ? (s / nrow) : 0.0);
            }
        }
    }

    // ---- Output FDs with metrics (support/confidence) ----
    void output(std::ostream& out) {
        size_t i = 0;
        for (const auto& fd : FD) {
            std::string result;
            for (auto x : fd) {
                result += std::to_string(x);
                result += " ";
            }
            if (!result.empty()) {
                result.replace(result.rfind(' '), 1, "");
                result.replace(result.rfind(' '), 1, " -> ");
            }

            double supp = (i < FD_support_.size()    ? FD_support_[i]    : 0.0);
            double conf = (i < FD_confidence_.size() ? FD_confidence_[i] : 0.0);

            char buf[128];
            std::snprintf(buf, sizeof(buf), " | supp=%.6f, conf=%.6f", supp, conf);

            out << result << buf << '\n';
            ++i;
        }
    }

    // Helpers
    void swap(int& a, int& b) { int t = a; a = b; b = t; }

    void quickSort(std::vector<int>& arr, int low, int high) {
        if (low < high) {
            int pivot = arr[high];
            int i = (low - 1);
            for (int j = low; j <= high - 1; j++) {
                if (cov_order_map[bitmap.at(arr[j])][0] >= cov_order_map[bitmap.at(pivot)][0]) {
                    i++; swap(arr[i], arr[j]);
                }
            }
            swap(arr[i + 1], arr[high]);
            quickSort(arr, low, i);
            quickSort(arr, i + 1, high);
        }
    }

    double calculateEuclideanDistance(const std::vector<int>& p1, const std::vector<int>& p2) {
        double d = 0.0;
        for (size_t i = 0; i < p1.size(); ++i) {
            double diff = p1[i] - p2[i];
            d += diff * diff;
        }
        return std::sqrt(d);
    }

    std::vector<std::vector<int>> mergeClusters(const std::vector<std::vector<int>>& c1,
                                                const std::vector<std::vector<int>>& c2) {
        std::vector<std::vector<int>> merged = c1;
        merged.insert(merged.end(), c2.begin(), c2.end());
        return merged;
    }

    // Cluster centroid (toy/unused in FD mining)
    std::vector<int> clusterCentroid(const std::vector<std::vector<int>>& cluster) {
        std::vector<int> centroid(cluster[0].size(), 0);
        for (const auto& row : cluster) {
            for (size_t f = 0; f < row.size(); ++f) centroid[f] += row[f];
        }
        for (size_t f = 0; f < centroid.size(); ++f) centroid[f] /= int(cluster.size());
        return centroid;
    }

    // Simple agglomerative clustering (toy)
    void cluster() {
        std::vector<std::vector<std::vector<int>>> clusters;
        clusters.reserve(data.size());
        for (const auto& point : data) clusters.push_back({point});

        const int num_clusters = 3;
        while ((int)clusters.size() > num_clusters) {
            double best = std::numeric_limits<double>::max();
            size_t i1 = 0, i2 = 0;
            for (size_t i = 0; i < clusters.size(); ++i) {
                for (size_t j = i + 1; j < clusters.size(); ++j) {
                    double d = std::numeric_limits<double>::max();
                    for (const auto& p1 : clusters[i]) {
                        for (const auto& p2 : clusters[j]) {
                            d = std::min(d, calculateEuclideanDistance(p1, p2));
                        }
                    }
                    if (d < best) { best = d; i1 = i; i2 = j; }
                }
            }
            auto merged = mergeClusters(clusters[i1], clusters[i2]);
            clusters.erase(clusters.begin() + i1);
            clusters.erase(clusters.begin() + i2 - 1);
            clusters.push_back(std::move(merged));
        }

        // Majority features (not used downstream)
        for (const auto& c : clusters) {
            std::vector<double> prop(c[0].size(), 0.0);
            for (const auto& r : c) {
                for (size_t f = 0; f < r.size(); ++f) prop[f] += r[f];
            }
            for (double& v : prop) v /= c.size();
            (void)prop; // silence unused warning
        }
    }

    // Main entry to extract FDs
    void extraction() {
        compute_cov();

        // Build single-column partitions, mark unique columns
        for (int i = 0; i < ncol; i++) {
            set_part_map[bitmap.at(i)] = std::move(one_column_partition(i));
            double num = double(set_part_map[bitmap.at(i)].size()) / (nrow ? nrow : 1);
            if (num < SUPPORT) {
                tabu_for_unique_cols |= bitmap.at(i);
            } else {
                non_unique_cols.push_back(i);
            }
        }

        // For each RHS, find minimal LHSs and record FDs + metrics
        for (auto can : non_unique_cols) {
            current_rhs = can;
            findLHSs();
            for (NodeIndex j : minDeps) {
                auto fd = getColIndexVector(j);
                for (auto& x : fd) x += 1;
                fd.push_back(can + 1);
                FD.emplace_back(fd);

                // store support/conf to align with FD order
                auto it = metrics_map_.find(j);
                if (it != metrics_map_.end()) {
                    FD_support_.push_back(it->second.first);
                    FD_confidence_.push_back(it->second.second);
                } else {
                    FD_support_.push_back(0.0);
                    FD_confidence_.push_back(0.0);
                }
            }

            // reset node flags for next RHS
            for (auto& n : NodeSet) {
                if (n.isVisited) {
                    n.isVisited = false;
                    n.isCandidateMinDep = true;
                    n.isCandidateMaxNonDep = true;
                    n.isDep = false;
                    n.isNonDep = false;
                    n.isMinDep = false;
                    n.isMaxNonDep = false;
                    n.isErrorNonDep = false;
                }
            }
            minDeps.clear();
            maxNonDeps.clear();
            tabu_for_unuse_cols = 0;
            select_cols.clear();
            ErrorNonDeps.clear();
            cov_order_map.clear();
            metrics_map_.clear();
        }

        // Sort FDs lexicographically
        std::sort(FD.begin(), FD.end(),
            [](const std::vector<int>& lhs, const std::vector<int>& rhs) -> bool {
                auto it1 = lhs.begin(), it2 = rhs.begin();
                while (it1 != lhs.end() && it2 != rhs.end()) {
                    if (*it1 < *it2) return true;
                    if (*it1 > *it2) return false;
                    ++it1; ++it2;
                }
                return (it2 != rhs.end());
            });
    }

    std::vector<std::vector<int>>& getFD() { return FD; }

private:
    std::vector<std::vector<int>> data;
    int nrow{};
    int ncol{};
    int nrowk{};
    int tabu_for_unique_cols{};
    int tabu_for_unuse_cols{};
    int current_rhs{};
    std::vector<std::vector<int>> FD;
    std::vector<int> non_unique_cols;
    std::vector<int> select_cols;
    std::vector<std::vector<double>> cov;
    std::vector<Node> NodeSet;
    std::vector<NodeIndex> minDeps;
    std::vector<NodeIndex> maxNonDeps;
    std::vector<NodeIndex> ErrorNonDeps;
    std::stack<NodeIndex> trace;

    std::unordered_map<int, std::vector<ColIndex>> set_part_map;         // nodeID -> label vector (partition as labels)
    std::unordered_map<NodeIndex, std::vector<double>> cov_order_map;     // nodeID -> [score, flag]

    // Thresholds
    double support_thr_;
    double conf_thr_;

    // Per-FD metrics (aligned with FD push order)
    std::vector<double> FD_support_;
    std::vector<double> FD_confidence_;

    // For current RHS: nodeID -> (support, confidence)
    std::unordered_map<NodeIndex, std::pair<double,double>> metrics_map_;

    // Enumerate subsets of nodeID (1-layer if depth=1)
    void subset(NodeIndex n, std::vector<NodeIndex>& S, int depth = -1) {
        if (depth == -1) depth = 32;
        if (depth == 0 || n == 0) return;
        NodeIndex nn = n;
        while (nn != 0) {
            NodeIndex firstOne = nn & ~(nn - 1);
            if ((n & ~firstOne) == 0) break;
            nn = nn & ~firstOne;
            S.push_back(n & ~firstOne);
            subset(n & ~firstOne, S, depth - 1);
        }
    }

    // Enumerate supersets of n within mask, avoiding tabu
    void superset(int n, std::vector<NodeIndex>& S, NodeIndex tabu, NodeIndex mask, int depth = -1) {
        if (depth == -1) depth = 32;
        if (depth == 0 || (n | tabu) == mask) return;
        NodeIndex nn = mask & ~n;
        while (nn != 0) {
            NodeIndex firstOne = nn & ~(nn - 1);
            nn = nn & ~firstOne;
            if ((firstOne & tabu) == 0) {
                S.push_back(n | firstOne);
                superset(n | firstOne, S, tabu, mask, depth - 1);
            }
        }
    }

    std::vector<int> getColIndexVector(NodeIndex nodeID) {
        std::vector<int> V;
        for (int i = 0; i < (int)bitmap.size(); ++i)
            if (nodeID & bitmap.at(i)) V.push_back(i);
        return V;
    }

    // Remove non-minimal seeds
    void minimize(std::vector<NodeIndex>& newSeeds, std::vector<NodeIndex>& seeds) {
        seeds.clear();
        while (!newSeeds.empty()) {
            auto x = *newSeeds.begin();
            newSeeds.erase(newSeeds.begin());
            bool keep = true;
            for (auto it = newSeeds.begin(); it != newSeeds.end();) {
                if ((*it & x) == x) { it = newSeeds.erase(it); }
                else if ((*it & x) == *it) { keep = false; break; }
                else { ++it; }
            }
            if (keep) seeds.push_back(x);
        }
        newSeeds.clear();
    }

    void findLHSs() {
        if (non_unique_cols.size() < 2) return;

        std::vector<NodeIndex> seeds;
        double num = double(set_part_map[bitmap.at(current_rhs)].size()) / (nrow ? nrow : 1);
        double bound = (1 - num) * SUPPORT - MAX_ERROR; (void)bound; // currently unused but kept

        for (auto col : non_unique_cols) {
            if (col == current_rhs) continue;
            select_cols.push_back(col);
            cov_order_map[bitmap.at(col)].push_back(cov[col][current_rhs]);
            cov_order_map[bitmap.at(col)].push_back(1.0);
        }
        if (!select_cols.empty())
            quickSort(select_cols, 0, (int)select_cols.size() - 1);

        for (auto non_col : select_cols) seeds.push_back(bitmap.at(non_col));

        while (!seeds.empty()) {
            while (!seeds.empty()) {
                NodeIndex nodeID = seeds.front();
                seeds.erase(seeds.begin());
                while (nodeID != -1) {
                    auto& node = NodeSet.at(nodeID);
                    if (node.isVisited) {
                        if (node.isCandidateMinDep || node.isCandidateMaxNonDep) {
                            if (node.isDep) {
                                if (node.isMinDep) {
                                    node.isCandidateMinDep = false;
                                    minDeps.push_back(nodeID);
                                }
                            } else {
                                if (node.isMaxNonDep) {
                                    node.isCandidateMaxNonDep = false;
                                    maxNonDeps.push_back(nodeID);
                                }
                            }
                        }
                    } else {
                        if (!inferCategory(nodeID)) computePartition(nodeID);
                    }
                    nodeID = pickNextNode(nodeID);
                }
            }
            seeds = generateNextSeeds();
        }
    }

    NodeIndex pickNextNode(NodeIndex nodeID) {
        auto& node = NodeSet.at(nodeID);
        if (node.isCandidateMinDep) {
            std::vector<NodeIndex> S;
            subset(nodeID, S, 1);
            for (auto it = S.begin(); it != S.end();) {
                if (NodeSet.at(*it).isVisited) {
                    if (NodeSet.at(*it).isNonDep) it = S.erase(it);
                    else if (NodeSet.at(*it).isDep) {
                        node.isCandidateMinDep = false; S.clear(); break;
                    } else ++it;
                } else ++it;
            }
            if (S.empty()) {
                if (node.isCandidateMinDep) {
                    node.isMinDep = true;
                    node.isCandidateMinDep = false;
                    minDeps.push_back(nodeID);
                }
            } else {
                NodeIndex nextNode = selectmax(S);
                trace.push(nodeID);
                return nextNode;
            }
        } else if (node.isCandidateMaxNonDep) {
            std::vector<NodeIndex> S;
            superset(nodeID, S, tabu_for_unique_cols | bitmap.at(current_rhs) | tabu_for_unuse_cols, (1 << ncol) - 1, 1);
            for (auto it = S.begin(); it != S.end();) {
                if (NodeSet.at(*it).isVisited) {
                    if (NodeSet.at(*it).isDep || NodeSet.at(*it).isErrorNonDep) it = S.erase(it);
                    else if (NodeSet.at(*it).isNonDep) {
                        node.isCandidateMaxNonDep = false; S.clear(); break;
                    } else ++it;
                } else ++it;
            }
            if (S.empty()) {
                if (node.isCandidateMaxNonDep) {
                    node.isMaxNonDep = true;
                    node.isCandidateMaxNonDep = false;
                    maxNonDeps.push_back(nodeID);
                }
            } else {
                NodeIndex nextNode = selectmax(S);
                trace.push(nodeID);
                return nextNode;
            }
        }

        NodeIndex idx = 0;
        while (!trace.empty()) {
            idx = trace.top();
            if (NodeSet.at(idx).isVisited && !NodeSet.at(idx).isCandidateMaxNonDep && !NodeSet.at(idx).isCandidateMinDep) {
                trace.pop();
            } else break;
        }
        if (trace.empty()) return -1;
        trace.pop();
        return idx;
    }

    ColIndex selectmax(std::vector<NodeIndex> S) {
        NodeIndex nextNode = 0;
        double max_score = -1.0;
        for (auto it = S.begin(); it != S.end(); ++it) {
            build_cov_map(*it);
            if (cov_order_map[*it][0] > max_score) {
                max_score = cov_order_map[*it][0];
                nextNode = *it;
            }
        }
        return nextNode;
    }

    std::vector<NodeIndex> generateNextSeeds() {
        std::vector<NodeIndex> seeds, newSeeds;
        for (auto maxNonDep : maxNonDeps) {
            NodeIndex complement = (bitmap.at(ncol) - 1) & (~maxNonDep) &
                                   ~(tabu_for_unique_cols | bitmap.at(current_rhs) | tabu_for_unuse_cols);
            if (seeds.empty()) {
                for (ColIndex i = 0; i < ncol; ++i)
                    if (complement & bitmap.at(i)) seeds.push_back(bitmap.at(i));
            } else {
                for (auto dep : seeds) {
                    for (ColIndex i = 0; i < ncol; ++i)
                        if (complement & bitmap.at(i)) newSeeds.push_back(dep | bitmap.at(i));
                }
                minimize(newSeeds, seeds);
            }
        }

        for (auto dep : minDeps) {
            auto it = std::find(seeds.begin(), seeds.end(), dep);
            if (it != seeds.end()) seeds.erase(it);
        }
        for (auto e : ErrorNonDeps) {
            auto it = std::find(seeds.begin(), seeds.end(), e);
            if (it != seeds.end()) seeds.erase(it);
        }
        return seeds;
    }

    void build_set(NodeIndex nodeID) {
        if (set_part_map.find(nodeID) != set_part_map.end()) return;
        std::vector<NodeIndex> S;
        subset(nodeID, S, 1);
        int childNodeID[2] = { -1, -1 };
        int idx = 0;
        for (auto s = S.begin(); s != S.end(); ++s) {
            if (set_part_map.find(*s) != set_part_map.end()) {
                childNodeID[idx++] = *s;
                if (idx == 2) break;
            }
        }
        if (idx == 2) {
            multiply_partitions(set_part_map[childNodeID[0]], set_part_map[childNodeID[1]], set_part_map[nodeID]);
        } else if (idx == 1) {
            multiply_partitions(set_part_map[nodeID & ~childNodeID[0]], set_part_map[childNodeID[0]], set_part_map[nodeID]);
        } else {
            auto lhs = getColIndexVector(nodeID);
            auto e = bitmap.at(lhs.back());
            lhs.erase(lhs.end() - 1);
            for (auto l : lhs) {
                auto x = bitmap.at(l);
                if (set_part_map.find(e | x) == set_part_map.end()) {
                    multiply_partitions(set_part_map[e], set_part_map[x], set_part_map[e | x]);
                }
                e |= x;
            }
        }
    }

    void build_cov_map(NodeIndex nodeID) {
        if (cov_order_map.find(nodeID) != cov_order_map.end()) return;
        std::vector<NodeIndex> S;
        subset(nodeID, S, 1);
        NodeIndex childNodeID[2] = { -1, -1 };
        std::vector<NodeIndex> reminS;
        int idx = 0;
        for (auto s = S.begin(); s != S.end(); ++s) {
            if (cov_order_map.find(*s) != cov_order_map.end() && cov_order_map[*s][1] == 1.0) {
                childNodeID[idx++] = *s;
                if (idx == 2) break;
            } else if (cov_order_map.find(*s) != cov_order_map.end()) {
                reminS.push_back(*s);
            }
        }
        if (idx == 2) {
            cov_compute(childNodeID[0], childNodeID[1], cov_order_map[nodeID]);
        } else if (idx == 1) {
            if (!reminS.empty()) cov_compute(reminS[0], childNodeID[0], cov_order_map[nodeID]);
            else                 cov_compute(nodeID & ~childNodeID[0], childNodeID[0], cov_order_map[nodeID]);
        } else {
            if (reminS.size() == 2)       cov_compute(reminS[0], reminS[1], cov_order_map[nodeID]);
            else if (reminS.size() == 1)  cov_compute(nodeID & ~reminS[0], reminS[0], cov_order_map[nodeID]);
            else {
                auto lhs = getColIndexVector(nodeID);
                auto e = bitmap.at(lhs.back());
                lhs.erase(lhs.end() - 1);
                for (auto l : lhs) {
                    auto x = bitmap.at(l);
                    if (cov_order_map.find(e | x) == cov_order_map.end()) {
                        cov_compute(e, x, cov_order_map[e | x]);
                    }
                    e |= x;
                }
            }
        }
    }

    void cov_compute(NodeIndex c0, NodeIndex c1, std::vector<double>& buf) {
        buf.reserve(2);
        NodeIndex n = c0 | c1;
        NodeIndex ii = ~c0 & n;
        NodeIndex jj = ~c1 & n;
        int r = -1, l = -1;
        for (ColIndex i = 0; i < (ColIndex)bitmap.size(); ++i) {
            if (ii & bitmap.at(i)) r = i;
            if (jj & bitmap.at(i)) l = i;
        }
        double x = cov_order_map[c0][0] + cov_order_map[c1][0] + cov[l][r];
        double b = x / 3.0;
        buf.push_back(b);
        buf.push_back(0.0);
    }

    // Try to infer category from neighbors
    bool inferCategory(NodeIndex nodeID) {
        auto& node = NodeSet.at(nodeID);
        std::vector<NodeIndex> S;
        subset(nodeID, S, 1);
        for (auto s : S) {
            if (NodeSet.at(s).isVisited && NodeSet.at(s).isDep) {
                node.isVisited = true; node.isDep = true;
                node.isCandidateMinDep = false; node.isCandidateMaxNonDep = false;
                return true;
            }
            if (NodeSet.at(s).isVisited && NodeSet.at(s).isErrorNonDep) {
                node.isVisited = true; node.isErrorNonDep = true;
                node.isCandidateMaxNonDep = false; node.isCandidateMinDep = false;
                return true;
            }
        }
        S.clear();
        superset(nodeID, S, tabu_for_unique_cols | bitmap.at(current_rhs) | tabu_for_unuse_cols, (1 << ncol) - 1, 1);
        for (auto s : S) {
            if (NodeSet.at(s).isVisited && NodeSet.at(s).isNonDep) {
                node.isVisited = true; node.isNonDep = true;
                node.isCandidateMaxNonDep = false; node.isCandidateMinDep = false;
                return true;
            }
        }
        return false;
    }

    // Pair-mass helpers (kept for potential future switch to pair metric)
    inline long long pair_mass_from_labels(const std::vector<ColIndex>& labels) {
        std::unordered_map<ColIndex, int> freq;
        freq.reserve(labels.size() * 2);
        for (ColIndex g : labels) ++freq[g];
        long long s = 0;
        for (const auto& kv : freq) {
            long long c = (long long)kv.second;
            s += c * (c - 1);
        }
        return s;
    }

    inline double support_pairs_from_labels(const std::vector<ColIndex>& labels, long long nrow_) {
        if (nrow_ <= 1) return 0.0;
        return (double)pair_mass_from_labels(labels) / ((double)nrow_ * (nrow_ - 1));
    }

    inline double error_pairs_from_labels(const std::vector<ColIndex>& labX,
                                          const std::vector<ColIndex>& labXY,
                                          long long nrow_) {
        if (nrow_ <= 1) return 0.0;
        long long sX  = pair_mass_from_labels(labX);
        long long sXY = pair_mass_from_labels(labXY);
        return (double)(sX - sXY) / ((double)nrow_ * (nrow_ - 1));
    }

    inline double confidence_from_labels(const std::vector<ColIndex>& labX,
                                         const std::vector<ColIndex>& labXY) {
        long long sX = pair_mass_from_labels(labX);
        if (sX == 0) return 0.0;
        long long sXY = pair_mass_from_labels(labXY);
        return (double)sXY / (double)sX;
    }

    // Compute partition, evaluate support/error/confidence, and set node state
    void computePartition(NodeIndex nodeID) {
        auto& node = NodeSet.at(nodeID);
        node.isVisited = true;

        build_set(nodeID);
        build_set(nodeID | bitmap.at(current_rhs));

        // Old cardinality-based metrics (kept, with safety guards)
        double a = (double)set_part_map[nodeID].size();
        double b = (double)set_part_map[nodeID | bitmap.at(current_rhs)].size();
        double num = (double)set_part_map[nodeID | bitmap.at(current_rhs)].size() / (double)(nrow ? nrow : 1);
        double ka  = (double)set_part_map[bitmap.at(current_rhs)].size() / (double)(nrow ? nrow : 1);

        // error = (a - b) / nrow; conf = b / a   (guard a/b to avoid division by zero)
        double error = (nrow ? (a - b) / nrow : 0.0);
        double conf  = (a > 0.0 ? (b / a) : 0.0);

        // error threshold driven by conf_thr_
        // original: error_th = num * (1.0 - CONFIDENCE) / b;
        // now:     error_th = num * (1.0 - conf_thr_) / max(b,1)
        double denom_b = (b > 0.0 ? b : 1.0);
        double error_th = num * (1.0 - conf_thr_) / denom_b;

        // record (support, confidence) for this nodeID
        metrics_map_[nodeID] = std::make_pair(num, conf);

        // score for ordering
        build_cov_map(nodeID);
        cov_order_map[nodeID][0] = (1.0 - ka) * (a / (double)(nrow ? nrow : 1)) - error;
        cov_order_map[nodeID][1] = 1.0;

        // Decide category with support/confidence thresholds
        if (error <= error_th && num >= support_thr_) {
            node.isDep = true;
            node.isCandidateMinDep = true;
            node.isNonDep = false;
            node.isCandidateMaxNonDep = false;
            node.isErrorNonDep = false;
        } else if (num < SUPPORT) {
            node.isDep = false;
            node.isCandidateMinDep = false;
            node.isNonDep = false;
            node.isCandidateMaxNonDep = false;
            node.isErrorNonDep = true;
            ErrorNonDeps.push_back(nodeID);
        } else {
            node.isDep = false;
            node.isCandidateMinDep = false;
            node.isNonDep = true;
            node.isCandidateMaxNonDep = true;
            node.isErrorNonDep = false;
        }

#ifdef PROPOGATE
        if (node.isDep) {
            std::vector<NodeIndex> S;
            superset(nodeID, S, bitmap.at(current_rhs) | tabu_for_unique_cols | tabu_for_unuse_cols, (1 << ncol) - 1, PROPOGATELAYER);
            for (NodeIndex s : S) {
                auto& nd = NodeSet.at(s);
                nd.isVisited = true; nd.isDep = true;
                nd.isCandidateMinDep = false; nd.isCandidateMaxNonDep = false;
            }
        } else if (node.isErrorNonDep) {
            std::vector<NodeIndex> S;
            superset(nodeID, S, bitmap.at(current_rhs) | tabu_for_unique_cols | tabu_for_unuse_cols, (1 << ncol) - 1, PROPOGATELAYER);
            for (NodeIndex s : S) {
                auto& nd = NodeSet.at(s);
                nd.isVisited = true; nd.isErrorNonDep = true;
                nd.isCandidateMinDep = false; nd.isCandidateMaxNonDep = false;
            }
        } else {
            std::vector<NodeIndex> S;
            subset(nodeID, S, PROPOGATELAYER);
            for (NodeIndex s : S) {
                auto& nd = NodeSet.at(s);
                nd.isVisited = true; nd.isNonDep = true;
                nd.isCandidateMinDep = false; nd.isCandidateMaxNonDep = false;
            }
        }
#endif
    }

    // One-column partition: keep row indices where value > 0 (binary-like)
    inline std::vector<ColIndex> one_column_partition(int col) {
        std::vector<ColIndex> ret;
        for (int ridx = 0; ridx < (int)data.size(); ++ridx) {
            auto cat = data[ridx][col];
            if (cat > 0) ret.push_back(ridx);
        }
        return ret;
    }

    // Intersect label vectors (as sets of row indices)
    inline void multiply_partitions(std::vector<ColIndex>& lhs,
                                    std::vector<ColIndex>& rhs,
                                    std::vector<ColIndex>& buf) {
        buf.resize(lhs.size());
        auto it = std::set_intersection(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), buf.begin());
        buf.resize(static_cast<size_t>(it - buf.begin()));
    }
};

#endif // FAST_AFD_H
