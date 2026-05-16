import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

path = "prototype/spark_experiment/mllib/results.csv"

df = pd.read_csv(path)

df = df[["engine", "input_file", "train_time_sec"]]

# Use dataset label from filename
df["size_gb"] = df["input_file"].str.replace("gb.parquet", "", regex=False).astype(float)
df = df.sort_values("size_gb")

pivot = df.pivot_table(
    index="size_gb",
    columns="engine",
    values="train_time_sec"
).reset_index()

pivot["size_label"] = pivot["size_gb"].map(lambda x: f"{x:g} GB")

x = np.arange(len(pivot))
width = 0.35

plt.figure(figsize=(7, 4.5))

plt.bar(x - width / 2, pivot["sklearn"], width, label="sklearn")
plt.bar(x + width / 2, pivot["spark_mllib"], width, label="Spark MLlib")

for i, v in enumerate(pivot["sklearn"]):
    plt.text(i - width / 2, v + 10, f"{v:.0f}s", ha="center", fontsize=9)

for i, v in enumerate(pivot["spark_mllib"]):
    plt.text(i + width / 2, v + 10, f"{v:.0f}s", ha="center", fontsize=9)

plt.xticks(x, pivot["size_label"])
plt.xlabel("Dataset size")
plt.ylabel("Training time (seconds)")
plt.title("Training Time Comparison")
plt.legend()
plt.grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.savefig("training_time_comparison.png", dpi=300)
print("Saved figure: training_time_comparison.png")
