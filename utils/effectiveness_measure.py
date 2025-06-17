def parse_FDs(filename):

    fds = set()
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue 
        
            left_str, right_str = line.split("->")
            left_str, right_str = left_str.strip(), right_str.strip()

     
            left_attrs = [x.strip() for x in left_str.split(',')]
            right_attrs = [x.strip() for x in right_str.split(',')]

            left_set = frozenset(left_attrs)
            right_set = frozenset(right_attrs)

            fds.add((left_set, right_set))

    return fds


def compute_recall_and_precision(benchmark_fds, found_fds):
    """
      - Recall    = TP / (TP + FN)
      - Precision = TP / (TP + FP)
    """
    TP = len(benchmark_fds & found_fds)   
    FP = len(found_fds - benchmark_fds)   
    FN = len(benchmark_fds - found_fds)   
    print(f"TP: {TP}, FP: {FP}, FN: {FN}")
    recall = TP / (TP + FN) if (TP + FN) else 0.0
    precision = TP / (TP + FP) if (TP + FP) else 0.0

    return recall, precision



if __name__ == "__main__":
    benchmark_file = r"utils\results\DFD_part_flight12.txt"
    found_file = r"utils\results\DFD_part_flight8.txt"

    # 1. 解析基准集
    benchmark_fds = parse_FDs(benchmark_file)

    # 2. 解析发现结果
    found_fds = parse_FDs(found_file)

    # 3. 计算 Recall 和 Precision
    recall, precision = compute_recall_and_precision(benchmark_fds, found_fds)
    f1 = 2 * recall * precision / (recall + precision) if (recall + precision) else 0.0
    print(f"Benchmark FDs count: {len(benchmark_fds)}")
    print(f"Found FDs count:     {len(found_fds)}")
    print(f"Recall:    {recall:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"F1:        {f1:.4f}")
