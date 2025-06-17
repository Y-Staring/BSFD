import pandas as pd
from sklearn.model_selection import train_test_split

def sampleData(in_path, out_path, sample_size = 0.3,dropNAN = True):
    data = pd.read_csv(in_path)
    max_class = -1
    # min_class = 1000000
    rep_attr = None
    print(data.shape)
    data.dropna(inplace=True)
    # for attr in data.columns.values:
    #     data = data.groupby(attr).filter(lambda x: len(x) > 1)
        # 过滤掉样本少于2个的类别

    for attr in data.columns.values:
        val_class = data[attr].value_counts()
        val_class_counts = len(val_class)
        print(attr, val_class_counts)
        # print(grouped_counts_tuples)
        if val_class_counts > max_class and val_class_counts < data.shape[0]/2:
            max_class = val_class_counts
            rep_attr = attr
        # if val_class_counts < min_class:
        #     min_class = val_class_counts
        #     rep_attr = attr
    # if dropNAN:
    #     data_cleaned = data.dropna(subset=[rep_attr])
    #     data = data_cleaned
    # else:
    #     data[rep_attr] = data[rep_attr].fillna('none')
    # print(data.shape)

    print(rep_attr)
    grouped_counts_tuples = list(data.groupby(rep_attr).size().items())
    # print(grouped_counts_tuples)
    #尽量少丢失信息
    for item in grouped_counts_tuples:
        if item[1] <1:
            print("******",item)
    # stratified_sample, _ = train_test_split(data, test_size=sample_size, stratify=data[[rep_attr]])
    stratified_sample = data.groupby(rep_attr, group_keys=False).apply(lambda x: x.sample(frac=sample_size))
    print(stratified_sample.shape)
    stratified_sample.to_csv(out_path,index=False)
    return rep_attr

rep_attribute = sampleData(r'.\datasets\adult_clean.csv', r'sample\sampled_data\sampled_adult_less.csv')
print(rep_attribute)
