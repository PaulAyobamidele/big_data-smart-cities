import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Simulate sample data
np.random.seed(0)
timestamps = pd.date_range(start="2024-08-30", periods=100, freq="H")
temperatures = np.random.uniform(low=15, high=35, size=len(timestamps))
humidities = np.random.uniform(low=30, high=70, size=len(timestamps))
pressures = np.random.uniform(low=980, high=1050, size=len(timestamps))

# Create a DataFrame
data = pd.DataFrame(
    {
        "Timestamp": timestamps,
        "Temperature": temperatures,
        "Humidity": humidities,
        "Pressure": pressures,
    }
)

# Set Timestamp as index
data.set_index("Timestamp", inplace=True)

# Plotting
fig, axs = plt.subplots(3, 1, figsize=(10, 12), sharex=True)

# Temperature Plot
axs[0].plot(data.index, data["Temperature"], color="red")
axs[0].set_title("Temperature Over Time")
axs[0].set_ylabel("Temperature (°C)")

# Humidity Plot
axs[1].plot(data.index, data["Humidity"], color="blue")
axs[1].set_title("Humidity Over Time")
axs[1].set_ylabel("Humidity (%)")

# Pressure Plot
axs[2].plot(data.index, data["Pressure"], color="green")
axs[2].set_title("Pressure Over Time")
axs[2].set_ylabel("Pressure (hPa)")
axs[2].set_xlabel("Timestamp")

# Format Date
plt.gcf().autofmt_xdate()

# Show Plot
plt.tight_layout()
plt.show()
