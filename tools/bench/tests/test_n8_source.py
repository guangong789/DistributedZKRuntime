"""Static Go source guards only. No Go tools, tests, Barrier or crypto calls."""
import hashlib
import re
import unittest

from tools.bench import g4_crypto as crypto, policy_binding as b

FUNCTION_SHA = {'TestMiMCChainCPUArtifactAudit': '22a8a6f513d958dc91e05a2258535a79f0babfe8a74af59d30d2aaf3fb86a2bb',
 'pilotMeasure': '36294776fe25ed219c706c094cd56e7f7017c20ad3aa06fa016459043dc1bf4d',
 'pilotN2Plan': '8dbd9ca78ec6f1fe41bc0407684dc1ff37cb0a605c44797761087f475c642090'}
CLOSURE_SHA = {'compile': '6f27c9cbe8a1eb8da90660a0a835f7722dc7790ceba805ca4f2da7c15ad10edc',
 'pk_decode': 'aa17ec51591b2a24ffb45b1f02489469d0c0fa5884c1766cfe9ae6976558610b',
 'pk_serialize': '635f23380834b195368a1e5b47902e1ae8bdc6efc21083add1f10e949c60329d',
 'proof_serialize': '22668c3d00e71676dbc898024ea9303c3a775bf4a5d4f835a55375fd2c955e6b',
 'prove': 'bc2d570373e5198407f937a79f0fd025c94a0904c0ad92c94b7ea246da341cb5',
 'setup': '6047626470054eea9417eea81415aac29ed589a64e59c069635901624a9f6c51',
 'verify': 'd02a5d831fdfcc6ef73c6069addbb9c04e7736d2111ced0162e03cac20c44b08',
 'vk_decode': 'fb75e0178ed5048a752f70a40faa58f5289b2435e5b9aff5aa911408835aeba4',
 'vk_serialize': '3015052e71a2f54069a806461a06b252c1448117dcf3211bc241c953de4b0dd2',
 'witness': 'b7ec2a69892b55e9b38f251f0f72c7b15c8078eb74ec5122c2e258a39edc61ea'}


def function(text, name):
    start = text.index('func ' + name + '(')
    end = text.find('\nfunc ', start + 5)
    return text[start:end if end != -1 else len(text)]


class N8Source(unittest.TestCase):
    def test_historical_timing_and_audit_functions_unchanged(self):
        root = b.REPO / 'internal/zk/experimental/mimcchain'
        driver = (root/'pilot_driver_test.go').read_text()
        pilot = (root/'pilot_n2_test.go').read_text()
        for text, name in ((driver, 'pilotMeasure'), (pilot, 'pilotN2Plan'), (pilot, 'TestMiMCChainCPUArtifactAudit')):
            self.assertEqual(b.sha(function(text, name).encode()), FUNCTION_SHA[name], name)
        closures = dict(re.findall(r'do\("([^"]+)", func\(\) error \{(.*?)\}\)', pilot, re.S))
        for name, expected in CLOSURE_SHA.items():
            body = closures[name].replace('NewCircuit(n)', 'NewCircuit(2)').replace('Circuit{N: n', 'Circuit{N: 2')
            self.assertEqual(b.sha(body.encode()), expected, name)

    def test_explicit_entry_and_barrier_before_crypto(self):
        root = b.REPO / 'internal/zk/experimental/mimcchain'
        text = (root/'pilot_n2_test.go').read_text().split('// This separate process reserves')[0]
        self.assertIn('func TestMiMCChainCPUPilot(t *testing.T)', text)
        self.assertIn('case "2":\n\t\tn, constraints = 2, 661', text)
        self.assertIn('case "8":\n\t\tn, constraints = 8, 2641', text)
        self.assertIn('default:\n\t\tt.Fatal("pilot requires explicit N=2 or N=8")', text)
        self.assertIn('t.Run(fmt.Sprintf("N%d", n)', text)
        before, after = text.split('pilotStartBarrier("pilot")', 1)
        for operation in ('groth16.', 'frontend.Compile', 'frontend.NewWitness', 'ComputeDigest('):
            self.assertNotIn(operation, before)
        self.assertEqual(after.count('groth16.Setup('), 1)
        self.assertEqual(after.count('groth16.Prove('), 2)
        self.assertEqual(after.count('groth16.Verify('), 3)  # Includes expected rejection.
        self.assertNotIn('time.Sleep', text)
        self.assertIn('"length": n', text)
        self.assertIn('Length: n', text)
        self.assertIn('mimcchain-n8-groth16-v1', text)
        self.assertEqual(crypto.SELECTOR, '^TestMiMCChainCPUPilot$/^N8$')

    def test_profile_source_pins_and_no_new_barrier_protocol(self):
        self.assertEqual(set(crypto.GO_SHA256), set(crypto.GO_SOURCES))
        for name, sha in crypto.GO_SHA256.items():
            self.assertEqual(b.sha((b.REPO/name).read_bytes()), sha, name)
        barrier = (b.REPO/'internal/zk/experimental/mimcchain/pilot_start_barrier_test.go').read_text()
        self.assertIn('encoder.Encode(map[string]any{"version": 1, "event": "ready"', barrier)
        self.assertIn('release.Event != "release"', barrier)
        self.assertIn('"event": "released"', barrier)


if __name__ == '__main__':
    unittest.main()
