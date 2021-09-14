class TestCollector:
    async def test_collect_qsize(self, simple_flow):
        qsize_metrics = simple_flow._metrics_manager.exporter.target.queue_sizes.items
        assert len(qsize_metrics) == 4
        for name, value in qsize_metrics:
            assert value == 0
