
from databricks.connect import DatabricksSession

try:
    spark = DatabricksSession.builder.getOrCreate()
    print(spark.conf.get("spark.databricks.workspaceUrl"))    
except Exception as e:
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    print(spark.conf.get("spark.databricks.workspaceUrl"))

%sql
SELECT
  workspace_id,
  usage.sku_name,
  SUM(usage_quantity * list_prices.pricing.default) AS cost_usd
FROM
  system.billing.usage
INNER JOIN
  system.billing.list_prices
  ON usage.sku_name = list_prices.sku_name
  AND usage.usage_start_time >= list_prices.price_start_time
  AND (usage.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
WHERE
  workspace_id = '984752964297111'
  AND usage.usage_date BETWEEN '2025-11-01' AND '2025-11-30'
GROUP BY
  workspace_id,
  usage.sku_name