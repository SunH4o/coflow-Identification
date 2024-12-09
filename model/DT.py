import pandas as pd
from sklearn.tree import DecisionTreeClassifier, export_text
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# 读取 CSV 文件
FILE_PATH = 'train_dataset_generation/train_dataset_continue_5_small.csv'
df = pd.read_csv(FILE_PATH)

# 保留一位小数
df = df.round(2)

# 假设 CSV 文件中有特征列 'feature1', 'feature2', ..., 'featureN' 和目标列 'target'
# 请根据你的实际数据列名进行替换
features = ['feature1', 'feature2']  # 替换为你的特征列名
target = 'label'  # 替换为你的目标列名

# 提取特征和目标
X = df[features]
y = df[target]

# print(X.shape)
# print(y.shape)

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=1)

# 创建决策树模型
clf = DecisionTreeClassifier()

# 训练模型
clf.fit(X_train, y_train)

# 预测
y_pred = clf.predict(X_test)

# 计算准确率
accuracy = accuracy_score(y_test, y_pred)
print(f'Accuracy: {accuracy}')

# 获取决策树的深度
tree_depth = clf.get_depth()
print(f'Tree Depth: {tree_depth}')

# 获取决策树的判别条件
tree_rules = export_text(clf, feature_names=features)
print(tree_rules)
