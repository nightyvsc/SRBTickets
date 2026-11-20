import unittest

from metrics.aggregate import calculate_metrics


class MetricsAggregationTests(unittest.TestCase):
    def test_calculates_average_latency_and_transactions_per_minute(self) -> None:
        metrics = calculate_metrics(
            [
                {"_op_type": "QUERY", "_duration_ms": 10.0, "_ts": 60.0, "status": "OK"},
                {"_op_type": "QUERY", "_duration_ms": 20.0, "_ts": 120.0, "status": "OK"},
            ]
        )

        query_metrics = metrics["operations"]["QUERY"]
        self.assertEqual(metrics["total_operations"], 2)
        self.assertEqual(query_metrics["avg_ms"], 15.0)
        self.assertEqual(query_metrics["tpm"], 2.0)


if __name__ == "__main__":
    unittest.main()
