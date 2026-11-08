"""Check that the local soak fails closed on lost work and missing metrics."""
import copy
import unittest

from soak_metrics import container_counts, evaluate, memory_bytes


class AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.report = {"generator_skipped_requests": 0,
            "stages": [{"scheduled_requests": 10, "dispatched_requests": 10}],
            "request_errors": 0, "ingest_events_sent": 500, "ingest_events_accepted": 500,
            "drain": {"matched": True, "actual_events": 500, "pending": 0, "pending_mutations": 0},
            "duplicate": {"verified": True}, "renewal_verified": True}
        self.metrics = {"samples": 4, "failed_samples": 0, "max_pending_count": 100,
                        "final_receipt_rows": 1000, "max_receipt_rows": 1000}

    def test_resource_units(self):
        self.assertEqual(memory_bytes("1.5GiB"), 1610612736)
        self.assertEqual(memory_bytes("512MiB"), 536870912)
        self.assertEqual(memory_bytes("1.2MB"), 1200000)
        with self.assertRaises(ValueError):
            memory_bytes("--")

    def test_inventory_counts_do_not_publish_names(self):
        counts = container_counts(["private-service-db", "porcupine-online-source-example"])
        self.assertEqual(counts, {"test_drill": 1, "unrelated": 1})
        self.assertNotIn("private-service-db", str(counts))

    def test_success_requires_complete_dispatch_acceptance_and_sql(self):
        self.assertTrue(evaluate(self.report, self.metrics, {"max_pending_count": 200})["passed"])
        variants = []
        lost = copy.deepcopy(self.report)
        lost["ingest_events_accepted"] = 450
        variants.append(lost)
        underdispatched = copy.deepcopy(self.report)
        underdispatched["stages"][0]["dispatched_requests"] = 9
        variants.append(underdispatched)
        unreconciled = copy.deepcopy(self.report)
        unreconciled["drain"]["actual_events"] = 499
        variants.append(unreconciled)
        pending_mutation = copy.deepcopy(self.report)
        pending_mutation["drain"]["pending_mutations"] = 1
        variants.append(pending_mutation)
        for report in variants:
            self.assertFalse(evaluate(report, self.metrics, {"max_pending_count": 200})["passed"])

    def test_missing_samples_excess_resources_and_receipts_fail(self):
        for metrics, thresholds, limit in (
            ({"samples": 0, "failed_samples": 0}, {"max_pending_count": 200}, 0),
            ({**self.metrics, "failed_samples": 1}, {"max_pending_count": 200}, 0),
            (self.metrics, {"max_pending_count": 50}, 0),
            (self.metrics, {"max_pending_count": 200}, 999),
            ({**self.metrics, "max_receipt_rows": 1001}, {"max_pending_count": 200}, 1000),
        ):
            self.assertFalse(evaluate(self.report, metrics, thresholds, limit)["passed"])

    def test_sampling_must_cover_the_ingestion_window(self):
        report = {**self.report, "started_at": "1970-01-01T00:00:00+00:00", "ingest_elapsed_seconds": 600}
        metrics = {**self.metrics, "samples": 120, "first_sample_unix": 3,
                   "last_sample_unix": 605, "maximum_sample_gap_seconds": 5}
        thresholds = {"minimum_successful_samples": 108, "minimum_ingest_window_coverage": 0.9,
                      "maximum_sample_gap_seconds": 12.5}
        self.assertTrue(evaluate(report, metrics, thresholds)["passed"])
        for changes in ({"samples": 2}, {"first_sample_unix": 100},
                        {"last_sample_unix": 400}, {"maximum_sample_gap_seconds": 20}):
            self.assertFalse(evaluate(report, {**metrics, **changes}, thresholds)["passed"])


if __name__ == "__main__":
    unittest.main()
