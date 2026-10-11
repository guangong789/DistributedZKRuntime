#!/usr/bin/env python3
"""Isolated bootstrap checks: all issuance is confined to a disposable /tmp tree."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts/dev-pki/bootstrap.sh"
BASH = shutil.which("bash")
OPENSSL = shutil.which("openssl")
FILES = ["ca/ca.crt", "ca/ca.key"] + [
    f"{node}/{name}" for node in ("coordinator", "worker-1", "client-1")
    for name in ("cert.pem", "key.pem")
]


class BootstrapTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="dzkr-dev-pki-test-", dir="/tmp")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.valid = cls.root / "valid"
        # Match the developer's already-created empty directory layout.
        cls.valid.mkdir(mode=0o700)
        for node in ("ca", "coordinator", "worker-1", "client-1"):
            (cls.valid / node).mkdir(mode=0o700)
        cls.run_bootstrap(cls.valid, success=True)

    @classmethod
    def run_bootstrap(cls, output, success=False, env=None):
        result = subprocess.run([BASH, str(SCRIPT), "--out", str(output)],
                                cwd=cls.root, env=env, capture_output=True, text=True, timeout=60)
        if success:
            if result.returncode != 0:
                raise AssertionError(result.stderr)
            if "Development PKI created and verified:" not in result.stdout:
                raise AssertionError("successful invocation omitted completion message")
        else:
            if result.returncode == 0 or "Development PKI created and verified:" in result.stdout:
                raise AssertionError("failed invocation reported success")
        return result

    def openssl(self, *args, success=True):
        result = subprocess.run([OPENSSL, *args], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode == 0, success, result.stderr)
        return result.stdout

    def test_layout_permissions_and_no_temporary_files(self):
        self.assertEqual(sorted(str(p.relative_to(self.valid)) for p in self.valid.rglob("*") if p.is_file()), sorted(FILES))
        for path in [self.valid, *self.valid.rglob("*")]:
            self.assertFalse(path.is_symlink())
            self.assertEqual(path.stat().st_mode & 0o777, 0o700 if path.is_dir() else 0o600)

    def test_ca_constraints_and_self_signature(self):
        ca = str(self.valid / "ca/ca.crt")
        text = self.openssl("x509", "-in", ca, "-noout", "-ext", "basicConstraints,keyUsage")
        self.assertIn("CA:TRUE, pathlen:0", text)
        self.assertIn("Certificate Sign, CRL Sign", text)
        self.openssl("verify", "-no-CApath", "-no-CAstore", "-CAfile", ca, "-check_ss_sig", "-x509_strict", ca)

    def test_leaf_profiles_and_purposes(self):
        for node, suffix in (("coordinator", "coordinator/main"), ("worker-1", "worker/worker-1"), ("client-1", "client/client-1")):
            with self.subTest(node=node):
                cert = str(self.valid / node / "cert.pem")
                text = self.openssl("x509", "-in", cert, "-noout", "-ext", "subjectAltName,extendedKeyUsage,basicConstraints")
                self.assertEqual(text.count("URI:"), 1)
                self.assertIn("URI:spiffe://distributedzk.local/" + suffix, text)
                self.assertIn("CA:FALSE", text)
                self.assertIn("TLS Web Client Authentication", text)
                verify = ["verify", "-no-CApath", "-no-CAstore", "-CAfile", str(self.valid / "ca/ca.crt"), "-x509_strict"]
                self.openssl(*verify, "-purpose", "sslclient", cert)
                if node == "client-1":
                    self.assertNotIn("IP Address:", text)
                    self.assertNotIn("DNS:", text)
                    self.assertNotIn("TLS Web Server Authentication", text)
                    self.openssl(*verify, "-purpose", "sslserver", cert, success=False)
                else:
                    self.assertIn("IP Address:127.0.0.1", text)
                    self.assertIn("DNS:localhost", text)
                    self.assertIn("TLS Web Server Authentication", text)
                    self.openssl(*verify, "-purpose", "sslserver", "-verify_ip", "127.0.0.1", cert)
                    self.openssl(*verify, "-purpose", "sslserver", "-verify_hostname", "localhost", cert)

    def test_unique_keys_and_matching_pairs(self):
        public_keys = []
        for node in ("ca", "coordinator", "worker-1", "client-1"):
            cert, key = ("ca.crt", "ca.key") if node == "ca" else ("cert.pem", "key.pem")
            public = self.openssl("x509", "-in", str(self.valid / node / cert), "-pubkey", "-noout")
            # Only public key bytes leave OpenSSL; never print private PEM.
            matching = self.openssl("pkey", "-in", str(self.valid / node / key), "-pubout")
            self.assertEqual(public, matching)
            public_keys.append(public)
        self.assertEqual(len(set(public_keys)), 4)

    def test_go_rpcauth_compatibility(self):
        result = subprocess.run(["go", "run", "-mod=readonly", "./scripts/dev-pki", "-dir", str(self.valid)],
                                cwd=REPO, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Go rpcauth verification passed", result.stdout)

    def test_go_rejects_chain_valid_malformed_identity(self):
        identities = {
            "wrong-id": "URI:spiffe://distributedzk.local/worker/worker-2",
            "duplicate-uri": "URI:spiffe://distributedzk.local/worker/worker-1,URI:spiffe://distributedzk.local/worker/worker-1",
            "raw-scheme-alias": "URI:SPIFFE://distributedzk.local/worker/worker-1",
        }
        for name, identity in identities.items():
            with self.subTest(name=name):
                # Copy only this test's generated material, never private/pki/.
                output = self.root / name
                shutil.copytree(self.valid, output)
                extensions = self.root / (name + ".conf")
                extensions.write_text("[leaf]\n"
                                      "basicConstraints=critical,CA:FALSE\n"
                                      "keyUsage=critical,digitalSignature\n"
                                      "extendedKeyUsage=serverAuth,clientAuth\n"
                                      "subjectKeyIdentifier=hash\n"
                                      "authorityKeyIdentifier=keyid,issuer\n"
                                      "subjectAltName=" + identity + ",IP:127.0.0.1,DNS:localhost\n")
                csr = str(self.root / (name + ".csr"))
                self.openssl("req", "-new", "-batch", "-sha256", "-key", str(output / "worker-1/key.pem"),
                             "-out", csr, "-subj", "/CN=Development malformed identity fixture")
                cert = str(output / "worker-1/cert.pem")
                self.openssl("x509", "-req", "-sha256", "-days", "7", "-set_serial", "0x" + self.openssl("rand", "-hex", "16").strip(),
                             "-in", csr, "-CA", str(output / "ca/ca.crt"), "-CAkey", str(output / "ca/ca.key"),
                             "-extfile", str(extensions), "-extensions", "leaf", "-out", cert)
                self.openssl("verify", "-no-CApath", "-no-CAstore", "-CAfile", str(output / "ca/ca.crt"),
                             "-x509_strict", "-purpose", "sslserver", "-verify_ip", "127.0.0.1", cert)
                result = subprocess.run(["go", "run", "-mod=readonly", "./scripts/dev-pki", "-dir", str(output)],
                                        cwd=REPO, capture_output=True, text=True, timeout=60)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("Go rpcauth verification passed", result.stdout)

    def test_repeated_invocation_preserves_every_file(self):
        def snapshot():
            return [(p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns) for p in (self.valid / f for f in FILES)]
        before = snapshot()
        result = self.run_bootstrap(self.valid)
        self.assertIn("back", result.stderr)
        self.assertEqual(snapshot(), before)

    def test_missing_arguments_do_not_issue(self):
        for args, code in (([], 1), (["--help"], 0)):
            result = subprocess.run([BASH, str(SCRIPT), *args], cwd=self.root, capture_output=True, text=True)
            self.assertEqual(result.returncode, code)
            self.assertIn("Usage:", result.stdout + result.stderr)

    def test_symlinks_are_rejected(self):
        destination = self.root / "symlink-destination"
        destination.mkdir(mode=0o700)
        link = self.root / "output-link"
        link.symlink_to(destination, target_is_directory=True)
        self.run_bootstrap(link)
        self.run_bootstrap(link / "child")
        output = self.root / "node-symlink"
        output.mkdir(mode=0o700)
        (output / "coordinator").symlink_to(destination, target_is_directory=True)
        self.run_bootstrap(output)
        self.assertEqual(list(destination.iterdir()), [])

    def mock_environment(self, mode):
        mocks = self.root / ("mock-" + mode)
        mocks.mkdir(mode=0o700)
        env = dict(os.environ, PATH=str(mocks) + os.pathsep + os.environ["PATH"],
                   PKI_TEST_MODE=mode, PKI_TEST_OPENSSL=OPENSSL, PKI_TEST_LN=shutil.which("ln"))
        command = mocks / "openssl"
        command.write_text('''#!/usr/bin/env bash
set -eu
case "$PKI_TEST_MODE:$1" in
    unsupported:version) printf 'OpenSSL 1.1.1\n'; exit 0 ;;
    openssl-failure:x509) exit 71 ;;
    random-failure:rand) exit 72 ;;
    interrupted:genpkey) kill -TERM "$PPID"; exit 143 ;;
esac
exec "$PKI_TEST_OPENSSL" "$@"
''')
        command.chmod(0o700)
        if mode in ("publication-failure", "directory-target", "symlink-target"):
            target = self.root / ("external-target-" + mode)
            target.mkdir(mode=0o700)
            env["PKI_TEST_TARGET"] = str(target)
            command = mocks / "ln"
            command.write_text('''#!/usr/bin/env bash
set -eu
destination=${@: -1}
if [[ $PKI_TEST_MODE == publication-failure && $destination == */coordinator/cert.pem ]]; then exit 73; fi
if [[ $destination == */ca/ca.key ]]; then
    case $PKI_TEST_MODE in
        directory-target) mkdir -- "$destination" ;;
        symlink-target) "$PKI_TEST_LN" -s -- "$PKI_TEST_TARGET" "$destination" ;;
    esac
fi
exec "$PKI_TEST_LN" "$@"
''')
            command.chmod(0o700)
        return env

    def test_publication_refuses_directory_or_symlink_destinations(self):
        # Inject destination changes only in the disposable tree, after the
        # empty-output preflight. ln must never treat a key destination as a
        # directory or follow its symlink into another directory.
        for mode in ("directory-target", "symlink-target"):
            with self.subTest(mode=mode):
                output = self.root / mode
                env = self.mock_environment(mode)
                self.run_bootstrap(output, env=env)
                self.assertEqual(list(Path(env["PKI_TEST_TARGET"]).iterdir()), [])
                self.assertEqual([p for p in output.rglob("*") if p.is_file()], [])
                self.assertEqual(list(output.glob(".bootstrap.*")), [])

    def test_failure_and_interruption_cleanup(self):
        for mode in ("openssl-failure", "random-failure", "interrupted", "publication-failure", "unsupported"):
            with self.subTest(mode=mode):
                output = self.root / mode
                self.run_bootstrap(output, env=self.mock_environment(mode))
                if output.exists():
                    self.assertEqual([p for p in output.rglob("*") if p.is_file()], [])
                    self.assertEqual(list(output.glob(".bootstrap.*")), [])

    def test_missing_openssl_fails_before_output_creation(self):
        empty = self.root / "empty-path"
        empty.mkdir(mode=0o700)
        output = self.root / "missing-openssl"
        result = self.run_bootstrap(output, env=dict(os.environ, PATH=str(empty)))
        self.assertIn("missing: openssl", result.stderr)
        self.assertFalse(output.exists())

    def test_git_output_boundary(self):
        git_root = self.root / "other-repository"
        git_root.mkdir(mode=0o700)
        subprocess.run(["git", "init", "-q", str(git_root)], check=True)
        output = git_root / "credentials"
        self.run_bootstrap(output)
        self.assertFalse(output.exists())
        # Inspect ignore/tracking metadata only; never issue into persistent PKI.
        for name in FILES:
            relative = "private/pki/" + name
            self.assertEqual(subprocess.run(["git", "check-ignore", "-q", relative], cwd=REPO).returncode, 0)
            self.assertNotEqual(subprocess.run(["git", "ls-files", "--error-unmatch", relative], cwd=REPO,
                                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
