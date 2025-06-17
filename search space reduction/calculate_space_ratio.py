import os
import pandas as pd

def get_column_counts_from_folder(folder_path):
    """
    从给定文件夹中读取所有 .csv 文件，并统计每个文件的列数。

    返回:
        List[int]: 每个文件的列数列表
    """
    col_counts = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            filepath = os.path.join(folder_path, filename)
            try:
                df = pd.read_csv(filepath, nrows=1)  # 只读取一行，提升效率
                col_counts.append(len(df.columns))
            except Exception as e:
                print(f"Warning: Failed to read {filename}: {e}")
    return col_counts

def compute_reduction_ratio(R, subtable_columns):
    """
    计算搜索空间缩减比例（以百分比输出）

    参数:
        R (int): 原始表的列数 |R|
        subtable_columns (list of int): 子表的列数 [|R1|, |R2|, ..., |Rk|]

    返回:
        str: 缩减比例（百分比形式，保留两位小数）
    """
    original_space = 2 ** R
    sub_space_sum = sum(2 ** Ri for Ri in subtable_columns)
    reduction_ratio = (original_space - sub_space_sum) / original_space
    percent = reduction_ratio * 100
    return f"{percent:.2f}%"

# 示例使用方式
if __name__ == "__main__":
    # 原始表列数
    sub_data_paths = [
        r"./datasets/subsets/subadult/",
        r"./datasets/subsets/subhosp/",
        r"./datasets/subsets/substu/",
        r"./datasets/subsets/subflight/",
        r"./datasets/subsets/substatalog/",
        r"./datasets/subsets/subamazon/"
    ]
    Rs = [15,17,26,20,21,24]

    for i in range(len(Rs)):
        # 路径：替换为你的子表 CSV 文件夹路径
        folder_path = sub_data_paths[i]  # <<< 修改为你的实际路径

        # 获取子表列数
        subtable_columns = get_column_counts_from_folder(folder_path)
        print("子表列数：", subtable_columns)

        # 计算搜索空间缩减
        result = compute_reduction_ratio(Rs[i], subtable_columns)
        print("搜索空间缩减比例：", result)
