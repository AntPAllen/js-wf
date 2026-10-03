#!/usr/bin/env python3
"""Verify six source-equivalent sustained components, including targeted retries."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re

spec = importlib.util.spec_from_file_location("campaign", Path(__file__).with_name("check-sustained-mutation-campaign.py"))
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)
WORKFLOW = ".github/workflows/invariant-mutations-sustained.yml"
RUNNER = "scripts/check-invariant-mutations.py"


def revision(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("an exact source revision is required")
    return value


def execution(metadata, mode, job_id):
    source = revision(metadata.get("headSha"))
    jobs = [job for job in metadata.get("jobs", []) if job.get("name") == f"sustained ({mode})"]
    if (len(jobs) != 1 or type(job_id) is not int
            or jobs[0].get("databaseId") != job_id
            or (jobs[0].get("status"), jobs[0].get("conclusion")) != ("completed", "success")):
        raise ValueError(f"{mode}: required component job is not uniquely identified and successful")
    return source


def inputs(sources):
    selected = {path: body for path, body in sources.items()
                if path.endswith(".go") or path in ("go.mod", "go.sum")}
    if not {"go.mod", "go.sum"} <= selected.keys() or not any(path.endswith(".go") for path in selected):
        raise ValueError("Go/module source inventory is incomplete")
    return selected


def check(components, reference, base, duration="10m"):
    revision(reference)
    if duration != "10m":
        raise ValueError("the sustained six-component gate requires ten minutes")
    if (not isinstance(components, list) or len(components) != len(campaign.MODES)
            or any(not isinstance(component, dict) for component in components)
            or {component.get("mode") for component in components} != set(campaign.MODES)):
        raise ValueError("exactly one component for each of the six categories is required")
    cache = {reference: campaign.git_sources(reference)}
    reference_sources = cache[reference]
    reference_inputs = inputs(reference_sources)
    reference_mutations = campaign.recorded_mutations(reference_sources[RUNNER])
    results = []
    for component in sorted(components, key=lambda item: item["mode"]):
        mode = component["mode"]
        metadata_path = base / component["metadata"]
        metadata_raw = metadata_path.read_bytes()
        metadata = json.loads(metadata_raw)
        source = execution(metadata, mode, component["job_id"])
        log_path = base / component["job_log"]
        log_raw = log_path.read_bytes()
        log = log_raw.decode()
        if source not in log or "baseline passed; production mutation detected" not in log:
            raise ValueError(f"{mode}: checkout or successful runner result missing from job log")
        if source not in cache:
            cache[source] = campaign.git_sources(source)
        sources = cache[source]
        if inputs(sources) != reference_inputs:
            raise ValueError(f"{mode}: Go/module inputs differ from the reference revision")
        if sources[WORKFLOW] != reference_sources[WORKFLOW]:
            raise ValueError(f"{mode}: execution workflow differs from the reference revision")
        name = campaign.MODES[mode][0]
        recorded = campaign.recorded_mutations(sources[RUNNER])[name]
        expected = reference_mutations[name]
        if any(recorded[field] != expected[field] for field in ("file", "before", "after")):
            raise ValueError(f"{mode}: selected mutation differs from the reference definition")
        artifact = base / component["artifact_root"]
        review = campaign.check_category(artifact, mode, duration, source, sources)
        hashes = {}
        for path in sorted(artifact.rglob("*")):
            if path.is_symlink():
                raise ValueError("symlink in component evidence")
            if path.is_file():
                hashes[str(path.relative_to(artifact))] = hashlib.sha256(path.read_bytes()).hexdigest()
        results.append(dict(mode=mode, source=source, job_id=component["job_id"],
                            parent_status=metadata.get("status"),
                            parent_conclusion=metadata.get("conclusion"), review=review,
                            metadata_sha256=hashlib.sha256(metadata_raw).hexdigest(),
                            job_log_sha256=hashlib.sha256(log_raw).hexdigest(),
                            artifact_sha256=hashes))
    return dict(reference_revision=reference, duration=duration, categories=results,
                source_revisions=sorted({item["source"] for item in results}),
                verified_go_module_sha256={path: hashlib.sha256(body).hexdigest()
                                          for path, body in reference_inputs.items()},
                actual_pairs=6, clears_sustained_six_mutation_gate=True,
                qualifies_parent_campaigns=False, clears_full_release=False,
                scope="Six independently reviewed ten-minute components with identical Go/module inputs and selected mutations. Failed parent campaigns remain rejected; full matrix and 24h gates remain separate.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--components", type=Path, required=True, help="JSON list of six component inputs; paths resolve against this file")
    parser.add_argument("--reference", required=True, help="exact reference Git revision")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("use a fresh output path to avoid stale qualification results")
    report = check(json.loads(args.components.read_text()), args.reference, args.components.parent)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items()
                      if key not in ("categories", "verified_go_module_sha256")}, indent=2))
