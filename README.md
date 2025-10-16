# 🛍️ E-Commerce Sales Forecasting with PySpark

[![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)](https://spark.apache.org/docs/latest/api/python/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Project Status](https://img.shields.io/badge/Status-Completed-brightgreen.svg)]()

---

## 📌 Overview

This project builds a **sales forecasting pipeline** for a multinational e-commerce company using **PySpark**.
The system processes raw transactional data, transforms it into daily sales summaries, and trains a **Random Forest Regressor** to predict product demand.

By leveraging PySpark’s distributed computing capabilities, the pipeline efficiently handles large-scale retail data to deliver:

* Automated **data cleaning and feature extraction** (date features, categorical encoding).
* **Daily-level aggregation** across countries and products.
* A trained **forecasting model** to estimate quantities sold.
* Performance evaluation using **Mean Absolute Error (MAE)**.
* A forecast of total units expected to be sold during a key sales week (week 39 of 2011).

This workflow provides the **Sales & Operations Planning (S&OP)** team with insights to optimize promotions, manage stock levels, and prepare for high-demand periods.

---

## 📂 Project Structure

```plaintext
ecommerce-sales-forecasting/
│
├── data/
│   └── Online Retail.csv                     # Raw input dataset
│
├── scripts/
│   └── sales_forecast_pyspark.py             # End-to-end PySpark forecasting pipeline
│
├── outputs/
│   ├── predictions/                          # Model predictions & evaluation outputs
│   └── models/                               # Trained Spark ML model (optional)
│
├── README.md                                 # Project overview and instructions
├── requirements.txt                          # Python dependencies
└── LICENSE                                   # MIT License
```

---

## 🧠 Pipeline Steps

1. **Data Loading**

   * Read `Online Retail.csv` using Spark with automatic schema inference.

2. **Date Parsing**

   * Convert `InvoiceDate` to proper timestamp and derive calendar features:

     * `Year`, `Month`, `Day`, `Week`, `DayOfWeek`

3. **Aggregation**

   * Aggregate daily sales at `(Country, StockCode, InvoiceDate)` level.
   * Compute:

     * Total `Quantity`
     * Average `UnitPrice`

4. **Data Split**

   * Train on data ≤ `2011-09-25`
   * Test on data > `2011-09-25`

5. **Feature Engineering**

   * Encode categorical variables (`Country`, `StockCode`) with `StringIndexer`.
   * Combine features into a single vector for modeling.

6. **Modeling**

   * Train a **Random Forest Regressor** on aggregated sales data.

7. **Evaluation**

   * Compute **Mean Absolute Error (MAE)** on test predictions.

8. **Forecasting**

   * Predict total **quantity sold globally during week 39 of 2011**.

---

## 📊 Output

| Metric                                   | Value          |
| ---------------------------------------- | -------------- |
| **Mean Absolute Error (MAE)**            | `≈ 8.97`       |
| **Forecasted Quantity (Week 39 – 2011)** | `86,241 units` |

*(values shown are illustrative)*

---

## ⚙️ How to Run

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the Forecasting Script

```bash
python scripts/sales_forecast_pyspark.py
```

✅ Output:

* **Mean Absolute Error (MAE)** score for forecast accuracy.
* **Predicted total quantity sold during week 39 of 2011**.

---

## 🧩 Tech Stack

* **PySpark** → Data processing, MLlib modeling
* **Random Forest Regressor** → Quantity forecasting
* **Parquet/CSV** → Input-output data formats
* **Pandas** → Lightweight inspection of Spark outputs

---

## 📑 Example Schema

| Column                                      | Type    | Description               |
| ------------------------------------------- | ------- | ------------------------- |
| `InvoiceNo`                                 | string  | Transaction identifier    |
| `StockCode`                                 | string  | Product code              |
| `Description`                               | string  | Product name              |
| `Quantity`                                  | integer | Quantity per transaction  |
| `UnitPrice`                                 | double  | Price per unit            |
| `CustomerID`                                | string  | Unique customer ID        |
| `Country`                                   | string  | Customer country          |
| `InvoiceDate`                               | date    | Date of purchase          |
| `Year`, `Month`, `Week`, `Day`, `DayOfWeek` | integer | Derived calendar features |

---

## 🧠 Key Features

* Fully automated **ETL and feature engineering pipeline**
* Scalable modeling using **Spark MLlib Random Forest**
* Aggregated and time-based features (`Year`, `Month`, `Week`, `DayOfWeek`)
* Performance evaluation with **Mean Absolute Error**
* Week-level forecasting for business planning

---

## 🔍 Insights

* Forecasting results provide visibility into **seasonal demand spikes**.
* Aggregation by `Country` and `StockCode` allows **SKU-level trend analysis**.
* The distributed PySpark approach ensures **fast training even on large datasets**.

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE).

---

## ✨ Contributor

Developed by [@OmarAlhaz](https://github.com/OmarAlhaz).
Open to community contributions 🚀

---

Would you like me to create a `requirements.txt` for this one too (matching the PySpark + MLlib stack)?
