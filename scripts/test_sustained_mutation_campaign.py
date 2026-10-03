import base64
import copy
import importlib.util
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("campaign", Path(__file__).with_name("check-sustained-mutation-campaign.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CampaignTests(unittest.TestCase):
    def test_recorded_registry_preserves_historical_anchor_without_execution(self):
        source = (module.runner.ROOT / "scripts/check-invariant-mutations.py").read_text()
        current = module.recorded_mutations(source)
        historical = module.recorded_mutations(source.replace(
            "(result ScanResult, scanErr error)", "(ScanResult, error)"))
        self.assertNotEqual(current["skipped_start_reconciler"]["before"],
                            historical["skipped_start_reconciler"]["before"])
        self.assertEqual(module.recorded_mutations(source + '\nraise RuntimeError("must not execute")\n'), current)
        with self.assertRaises(ValueError):
            module.recorded_mutations(source.replace('dict(name="missing_cas"',
                                                      'dict(name=str(42)', 1))
        with self.assertRaises(ValueError):
            module.recorded_mutations(source.replace('name="missing_cas"',
                                                      'name="independent_worker_leases"', 1))

    def metadata(self):
        return dict(headSha="a"*40, status="completed", conclusion="success",
                    jobs=[dict(name=name, status="completed", conclusion="success") for name in ["categories"] + [f"sustained ({mode})" for mode in module.MODES]])

    def test_complete_metadata_and_rejected_missing_duplicate_failure_live_source(self):
        self.assertEqual(module.check_metadata(self.metadata()), "a"*40)
        original = self.metadata()
        cases = []
        for key, value in (("status", "in_progress"), ("conclusion", "failure"), ("headSha", "main")):
            changed = copy.deepcopy(original)
            changed[key] = value
            cases.append(changed)
        changed = copy.deepcopy(original)
        changed["jobs"].pop()
        cases.append(changed)
        changed = copy.deepcopy(original)
        changed["jobs"][-1] = changed["jobs"][-2]
        cases.append(changed)
        changed = copy.deepcopy(original)
        changed["jobs"][-1]["conclusion"] = "failure"
        cases.append(changed)
        for changed in cases:
            with self.assertRaises(ValueError):
                module.check_metadata(changed)

    def archived_outputs(self):
        archive_path = module.runner.ROOT / "docs/scale/sustained-mixed-mutations-2026-10-02/local-smoke/originals.tar.gz"
        outputs = {}
        with tarfile.open(archive_path) as archive:
            for category, name in (("cas", "missing_cas"), ("start", "skipped_start_reconciler")):
                for phase in ("baseline", "mutated"):
                    data = archive.extractfile(f"{category}/{name}-{phase}.jsonl").read().decode()
                    events = [json.loads(line) for line in data.splitlines() if line.startswith("{")]
                    module.require_execution(events, module.TEST, "pass" if phase == "baseline" else "fail")
                    outputs[category, phase] = module.output_for(events)
        return outputs

    def test_original_receipts_and_rejected_duplicate_cas_wrong_index_and_start_generation(self):
        outputs = self.archived_outputs()
        module.decode_proofs("cas", outputs["cas", "baseline"], outputs["cas", "mutated"])
        module.decode_proofs("start-repair", outputs["start", "baseline"], outputs["start", "mutated"])
        receipts = module.raw_receipts(outputs["cas", "mutated"], "MIXED_CAS_RAW_RECEIPT")
        changed = copy.deepcopy(receipts)
        changed[1]["Sequence"] = changed[0]["Sequence"]
        with self.assertRaises(ValueError):
            module.decode_proofs("cas", "", "\n".join("MIXED_CAS_RAW_RECEIPT " + json.dumps(raw) for raw in changed))
        changed = copy.deepcopy(receipts)
        body = json.loads(base64.b64decode(changed[0]["Data"]))
        body["index"] = 3
        changed[0]["Data"] = base64.b64encode(json.dumps(body).encode()).decode()
        with self.assertRaises(ValueError):
            module.decode_proofs("cas", "", "\n".join("MIXED_CAS_RAW_RECEIPT " + json.dumps(raw) for raw in changed))
        wrong_generation = outputs["start", "baseline"].replace("start:guardshort.repair-orphan:225", "start:guardshort.repair-orphan:224")
        with self.assertRaises(ValueError):
            module.decode_proofs("start-repair", wrong_generation, outputs["start", "mutated"])
        with self.assertRaises(ValueError):
            module.decode_proofs("cas", "", outputs["cas", "mutated"].replace("index 2 at position 3", "unrelated invariant"))

    def test_actual_execution_cannot_be_replaced_by_build_skip_timeout_or_duplicate(self):
        good = [dict(Test=module.TEST, Action="fail"), dict(Package="js-wf/integration", Action="fail")]
        module.require_execution(good, module.TEST, "fail")
        for changed in ([good[1]], [dict(Test=module.TEST, Action="skip"), good[1]], good + [good[0]],
                        good + [dict(Action="build-fail")], good + [dict(Output="panic: test timed out")],
                        [good[0], dict(Package="js-wf/integration", Action="pass")]):
            with self.assertRaises(ValueError):
                module.require_execution(changed, module.TEST, "fail")

    def test_missing_message_id_receipts_allow_null_headers_but_reject_hidden_ids(self):
        baseline = dict(Sequence=1, Data=base64.b64encode(b"guardshort.guardshort-0-0").decode(), Header={"Nats-Msg-Id": ["mixed-enqueue:guardshort.guardshort-0-0"]})
        raw = [dict(Sequence=index+2, Data=baseline["Data"], Header=None) for index in range(64)]
        positive = "MIXED_ENQUEUE_RAW_RECEIPT " + json.dumps(baseline)
        negative = "\n".join("MIXED_ENQUEUE_RAW_RECEIPT " + json.dumps(item) for item in raw)
        module.decode_proofs("enqueue", positive, negative)
        raw[0]["Header"] = baseline["Header"]
        with self.assertRaises(ValueError):
            module.decode_proofs("enqueue", positive, "\n".join("MIXED_ENQUEUE_RAW_RECEIPT " + json.dumps(item) for item in raw))

    def test_source_mismatch_is_rejected_before_any_claimed_green_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "report.json").write_text(json.dumps(dict(head="a"*40, duration="35s", release_duration=False, source_inventory_sha256s={"journal/journal.go": "f"*64})))
            with self.assertRaisesRegex(ValueError, "source inventory"):
                module.check_category(root, "cas", "35s", "a"*40, {"journal/journal.go": b"original production source"})


if __name__ == "__main__":
    unittest.main()
