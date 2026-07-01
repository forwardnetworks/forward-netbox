from django.template.loader import render_to_string
from django.test import TestCase


class ExecutionInsightsDeleteLabelTest(TestCase):
    """The per-model panel shows delete_count, which is PLANNED intent from the
    Forward snapshot diff (rows Forward stopped reporting), computed before apply.
    An item already absent from NetBox plans a delete but applies none, so this
    number legitimately differs from the applied num_deleted / change log. It must
    be labeled "planned" so it does not read as "48 deleted" and contradict an
    empty change log (the sync-23 confusion).
    """

    def _render(self):
        return render_to_string(
            "forward_netbox/inc/execution_insights.html",
            {
                "latest_execution_insights": {
                    "available": True,
                    "top_model_results": [
                        {
                            "model": "dcim.inventoryitem",
                            "query_name": "Forward Inventory Items",
                            "execution_mode": "query_path",
                            "fetch_mode": "nqe_parameters",
                            "row_count": 61129,
                            "delete_count": 48,
                        }
                    ],
                }
            },
        )

    def test_delete_count_is_labeled_planned(self):
        html = self._render()
        self.assertIn("48 delete(s) planned", html)

    def test_bare_deleted_label_is_gone(self):
        # The old "{n} deletes" label read as applied and contradicted the change
        # log; it must not reappear.
        html = self._render()
        self.assertNotIn("48 deletes", html)
