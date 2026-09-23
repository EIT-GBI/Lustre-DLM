import os
import stat
import subprocess
import sys
import textwrap
from pathlib import Path


LAUNCHER = Path(__file__).parents[1] / "stat_srun"


def fake_slurm(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "scontrol").write_text("#!/bin/sh\nprintf '%s\\n' fake-node\n")
    (bin_dir / "srun").write_text(textwrap.dedent("""
        #!/usr/bin/env python3
        import os, signal, sys, time

        role = next((x for x in sys.argv if x in ('bus', 'worker', 'collect', 'coordinator')), 'unknown')
        log = os.environ['FAKE_LOG']
        with open(log, 'a') as stream:
            stream.write('start:' + role + '\\n')

        def stop(signum, frame):
            with open(log, 'a') as stream:
                stream.write('term:' + role + '\\n')
            raise SystemExit(143)

        signal.signal(signal.SIGTERM, stop)
        if role == 'coordinator':
            time.sleep(float(os.environ.get('COORD_DELAY', '0.1')))
            raise SystemExit(int(os.environ.get('COORD_STATUS', '0')))
        if role == 'collect':
            time.sleep(float(os.environ.get('COLLECT_DELAY', '0.1')))
            with open(os.environ['COLLECT_MARKER'], 'w') as stream:
                stream.write('complete')
            raise SystemExit(0)
        while True:
            time.sleep(0.05)
    """).lstrip())
    for executable in bin_dir.iterdir():
        executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    return bin_dir, bin_dir / "srun"


def invoke(tmp_path: Path, *, coordinator_status: int = 0) -> tuple[subprocess.CompletedProcess, Path]:
    bin_dir, _ = fake_slurm(tmp_path)
    log = tmp_path / "events.log"
    marker = tmp_path / "collector.done"
    env = os.environ.copy()
    env.update({
        "PATH": f"{bin_dir}:{env['PATH']}",
        "SLURM_JOB_NODELIST": "fake-node",
        "FAKE_LOG": str(log),
        "COLLECT_MARKER": str(marker),
        "COORD_STATUS": str(coordinator_status),
        "COLLECT_DELAY": "0.2",
    })
    result = subprocess.run(
        [str(LAUNCHER), f"--prefix={tmp_path}", "--outfile=/dev/null"],
        env=env, text=True, capture_output=True, timeout=5,
    )
    return result, log


def test_waits_for_collector_and_cleans_owned_services(tmp_path: Path):
    result, log = invoke(tmp_path)
    assert result.returncode == 0, result.stderr
    events = log.read_text().splitlines()
    assert "start:coordinator" in events
    assert (tmp_path / "collector.done").read_text() == "complete"
    assert "term:bus" in events
    assert "term:worker" in events


def test_coordinator_failure_is_returned_and_services_are_cleaned(tmp_path: Path):
    result, log = invoke(tmp_path, coordinator_status=17)
    assert result.returncode == 17
    events = log.read_text().splitlines()
    assert "term:bus" in events
    assert "term:worker" in events
    assert "term:collect" in events
    assert not (tmp_path / "collector.done").exists()
