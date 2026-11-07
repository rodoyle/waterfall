"""Wire-format regression tests for the synthetic VITA49 tone sender."""

import importlib.util
import struct
import unittest
from pathlib import Path


spec = importlib.util.spec_from_file_location(
    "vita49_tone_sender", Path(__file__).resolve().parents[1] / "bin" / "vita49_tone_sender.py"
)
assert spec is not None and spec.loader is not None
sender = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sender)


class ToneSenderTest(unittest.TestCase):
    def test_packet_size_and_wrapping_sequence(self):
        for samples, count in [(2048, 0), (360, 15), (7, 16)]:
            with self.subTest(samples=samples, count=count):
                packet = sender.build_packet(0, 0, 512, 4096, 1024, samples, count)
                header = struct.unpack_from(">I", packet)[0]
                self.assertEqual(len(packet), (5 + samples) * 4)
                self.assertEqual(header & 0xFFFF, 5 + samples)
                self.assertEqual((header >> 16) & 15, count & 15)


if __name__ == "__main__":
    unittest.main()
