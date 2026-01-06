import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) > 2:
    input_file = sys.argv[1]
    output_file = sys.argv[2]
else:
    print("Usage: python script.py <input_csv> <output_image>")
    sys.exit(1)


df = pd.read_csv(input_file, sep=";", names=["ArchSource", "ArchTarget", "Count", "Win", "CountSrc", "CountTgt", "Prediction"])

correlation = df["Count"].corr(df["Prediction"])
print(f"Correlation Coefficient: {correlation}")

plt.figure(figsize=(10, 6))
plt.scatter(df["Prediction"], df["Count"], alpha=0.5, s=10)

# Add y=x reference line
max_val = max(df["Prediction"].max(), df["Count"].max())
plt.plot([0, max_val], [0, max_val], color='red', linestyle='--', label='y=x')

# Add linear regression line
m, b = np.polyfit(df["Prediction"], df["Count"], 1)
x_range = np.linspace(0, max_val, 100)
plt.plot(x_range, m * x_range + b, color='blue', linewidth=2, label=f'Regression (y={m:.2f}x + {b:.2f})')

plt.xlabel("Theory (Prediction)")
plt.ylabel("Observed reality (Count)")
plt.title(f"Verification of Matchmaking (Correlation: {correlation:.4f})")
plt.legend()
plt.grid(True)

plt.savefig(output_file)
print(f"Graph saved as '{output_file}'")