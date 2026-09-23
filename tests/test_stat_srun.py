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
        status = int(os.environ.get(role.upper() + '_STATUS', '0'))
        if role == 'coordinator':
            time.sleep(float(os.environ.get('COORD_DELAY', '0.1')))
            raise SystemExit(status)
        if role == 'collect':
            time.sleep(float(os.environ.get('COLLECT_DELAY', '0.1')))
            if status == 0:
                with open(os.environ['COLLECT_MARKER'], 'w') as stream:
                    stream.write('complete')
            raise SystemExit(status)
        if status:
            raise SystemExit(status)
        while True:
            time.sleep(0.05)
    """).lstrip())
    for executable in bin_dir.iterdir():
        executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    return bin_dir, bin_dir / "srun"


def invoke(tmp_path: Path, *, coordinator_status: int = 0, collector_status: int = 0, worker_status: int = 0) -> tuple[subprocess.CompletedProcess, Path]:
    bin_dir, _ = fake_slurm(tmp_path)
    log = tmp_path / "events.log"
    marker = tmp_path / "collector.done"
    env = os.environ.copy()
    env.update({
        "PATH": f"{bin_dir}:{env['PATH']}",
        "SLURM_JOB_NODELIST": "fake-node",
        "FAKE_LOG": str(log),
        "COLLECT_MARKER": str(marker),
        "COORDINATOR_STATUS": str(coordinator_status),
        "COLLECT_STATUS": str(collector_status),
        "WORKER_STATUS": str(worker_status),
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


def test_collector_failure_is_returned_and_services_are_cleaned(tmp_path: Path):
    result, log = invoke(tmp_path, collector_status=19)
    assert result.returncode == 19
    events = log.read_text().splitlines()
    assert "term:bus" in events
    assert "term:worker" in events


def test_early_worker_failure_is_returned(tmp_path: Path):
    result, log = invoke(tmp_path, worker_status=23)
    assert result.returncode == 23
    events = log.read_text().splitlines()
    assert "term:bus" in events
    assert "term:coordinator" in events
    assert "term:collect" in events


def test_sigterm_cleans_owned_jobs_but_not_unrelated_process(tmp_path: Path):
    bin_dir, _ = fake_slurm(tmp_path)
    log = tmp_path / "events.log"
    marker = tmp_path / "collector.done"
    sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"])
    env = os.environ.copy()
    env.update({
        "PATH": f"{bin_dir}:{env['PATH']}",
        "SLURM_JOB_NODELIST": "fake-node",
        "FAKE_LOG": str(log),
        "COLLECT_MARKER": str(marker),
        "COORD_DELAY": "10",
        "COLLECT_DELAY": "10",
    })
    process = subprocess.Popen(
        [str(LAUNCHER), f"--prefix={tmp_path}", "--outfile=/dev/null"],
        env=env, text=True,
    )
    try:
        for _ in range(100):
            if log.exists() and "start:coordinator" in log.read_text():
                break
            import time
            time.sleep(0.02)
        process.terminate()
        assert process.wait(timeout=5) == 143
        assert sentinel.poll() is None
        events = log.read_text().splitlines()
        assert "term:bus" in events
        assert "term:worker" in events
        assert "term:collect" in events
        assert "term:coordinator" in events
    finally:
        sentinel.terminate()
        sentinel.wait(timeout=5)
