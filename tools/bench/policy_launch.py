"""Small G2 probe adapter around the unchanged supervisor; single-threaded only."""
import argparse
import json
import os
from pathlib import Path
import sys

from . import policy_binding as binding
from . import policy_verify
from . import supervise


def launch(out):
    m, policy = binding.check_binding(out)
    out = Path(out)
    st = out.stat()
    binding.require(st.st_uid == os.getuid() and st.st_mode & 0o077 == 0, 'private owned evidence directory required')
    binding.require(m['evidence_directory'] == dict(path=str(out.resolve()), device=st.st_dev,
                                                  inode=st.st_ino, uid=st.st_uid), 'launch directory substituted')
    binding.require(m['cwd'] == str(binding.REPO) and m['tool_directory'] == str(binding.TOOLS)
                    and m['python'] == sys.executable, 'prepared invocation belongs to another installation')
    for name, ref in m['tool_sources'].items():
        binding.require(binding.sha(binding.read_regular(binding.TOOLS / name)) == ref['sha256'],
                        'tool source changed before launch')
    # O_EXCL makes this a one-shot launch. This is intent, never proof of spawn.
    request = dict(schema=1, run_id=m['run_id'], state='launch_requested',
                   manifest_sha256=binding.sha(binding.read_regular(out / 'manifest.json')))
    binding.write_private(out / 'execution.json', binding.encoded(request))
    previous = os.environ.copy()
    try:
        os.environ.clear()
        os.environ.update(m['environment'])
        supervise.supervise(m['target_argv'], out / 'supervised', role='probe',
                            env_overrides={'DZKR_MIMCCHAIN_BARRIER_PROBE': '1'},
                            barrier_seconds=policy['barrier_seconds'],
                            outer_seconds=policy['outer_seconds'], inner_seconds=policy['wall_seconds'])
    finally:
        os.environ.clear()
        os.environ.update(previous)
    # Reconstruct from disk, not the callable's return value. Missing evidence
    # never becomes a success. A crash before this write remains incomplete.
    outcome = policy_verify.observed_outcome(out, m, policy)
    binding.write_private(out / 'outcome.json', binding.encoded(outcome))
    return policy_verify.verify(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', required=True)
    parser.add_argument('--out', required=True, help='new private external directory')
    parser.add_argument('--run-id', help='optional unique 32-character lowercase hex ID')
    parser.add_argument('argv', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    argv = args.argv[1:] if args.argv[:1] == ['--'] else args.argv
    try:
        out = binding.prepare(args.policy, args.out, argv, args.run_id)
        result = launch(out)
        print(json.dumps(result))
        return 0 if result['run_succeeded'] else 1
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print('policy launch rejected: ' + (str(exc) if isinstance(exc, binding.PolicyError) else type(exc).__name__), file=sys.stderr)
        return 2


if __name__ == '__main__':
    os.umask(0o077)
    sys.exit(main())
