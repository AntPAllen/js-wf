assert '--- PASS: TestRawGraphSignalReservationAndBinding ' in log
assert '--- PASS: TestRawGraphSignalIndexProductionPackets ' in log
assert '--- PASS: TestRawGraphSignalIndexProductionPackets/maximum-depth ' in log
for control in ('valid', 'magic', 'header-reserved', 'truncated', 'count', 'leaf-key', 'leaf-value', 'leaf-child', 'bit-overflow', 'node-reserved', 'left-reserved', 'right-reserved', 'branch-value', 'branch-prefix', 'branch-bit-order', 'future-record', 'same-record-forward', 'old-slot-missing', 'wrong-side', 'lost-prefix', 'lost-one-old-key', 'unreachable-node', 'canceled'):
    assert f'--- PASS: TestRawGraphSignalIndexIndependentPrefixes/{control} ' in log
for control in ('index-magic', 'index-key', 'index-value', 'queue-index-key'):
    assert f'--- PASS: TestRawGraphSignalReservationAndBinding/{control} ' in log
for encoding in ('json', 'protobuf-v1'):
    assert f'--- PASS: TestRawGraphSignalReservationAndBinding/{encoding} ' in log
    assert f'--- PASS: TestRawGraphSignalReservationAndBinding/{encoding}/consumed-valid ' in log
for control in ('generation', 'reservation-index', 'name', 'key', 'size', 'body', 'token', 'binding-input', 'binding-index', 'source-zero', 'source-frontier', 'consumption-census', 'missing-body', 'consumed-index', 'consumed-token', 'consumed-name', 'consumed-sequence', 'consumed-ref', 'consumed-hash', 'consumed-no-marker', 'consumed-unowned'):
    assert f'--- PASS: TestRawGraphSignalReservationAndBinding/{control} ' in log
