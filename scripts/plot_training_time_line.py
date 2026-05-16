import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

path = "prototype/spark_experiment/mllib/results.csv"

df = pd.read_csv(path)

df = df[["engine", "input_file", "train_time_sec"]]

df["size_gb"] = (
    df["input_file"]
    .str.replace("gb.parquet", "", regex=False)
    .astype(float)
)

df = df.sort_values("size_gb")

pivot = df.pivot_table(
    index="size_gb",
    columns="engine",
    values="train_time_sec"
).reset_index()

pivot["size_label"] = pivot["size_gb"].map(lambda x: f"{x:g} GB")

plt.figure(figsize=(7.5, 4.7))

plt.plot(
    pivot["size_label"],
    pivot["sklearn"],
    marker="o",
    linewidth=2,
    label="sklearn"
)

plt.plot(
    pivot["size_label"],
    pivot["spark_mllib"],
    marker="s",
    linewidth=2,
    label="Spark MLlib"
)

# Add value labels with safe offsets
for i, v in enumerate(pivot["sklearn"]):
    offset = (0, -16) if i == 0 else (0, 8)

    plt.annotate(
        f"{v:.0f}s",
        (pivot["size_label"][i], v),
        textcoords="offset points",
        xytext=offset,
        ha="center",
        fontsize=9
    )

for i, v in enumerate(pivot["spark_mllib"]):
    offset = (0, 8) if i == 0 else (0, -16)

    plt.annotate(
        f"{v:.0f}s",
        (pivot["size_label"][i], v),
        textcoords="offset points",
        xytext=offset,
        ha="center",
        fontsize=9
    )

plt.xlabel("Dataset size")
plt.ylabel("Training time (seconds)")
plt.title("Training Time Comparison")
plt.legend()
plt.grid(axis="y", alpha=0.3)

# Add top margin so 620s does not hit the border
max_y = pivot[["sklearn", "spark_mllib"]].max().max()
plt.ylim(0, max_y * 1.18)

plt.tight_layout()
plt.savefig("training_time_line.png", dpi=300)
print("Saved figure: training_time_line.png")