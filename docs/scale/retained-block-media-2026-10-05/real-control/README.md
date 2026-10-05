# Real block filesystem retention and default cleanup controls accepted

Executed e453ed69e5eaf00fd0cd82e8f75a597aa948adad, actualraceSDK472174,
SHA256 `1de2a39c17eda659ac6c26f339b90614696d557a46c900615858a425a92b6c0b`.
Clean recorded source;668selectedGit/2642selectedexternal inputs before/after
verified. Both realLinux device tests pass: ordinary5s stall plus canceled-stall
cleanup5.55s, retainedclosedfilesystem copy/read-only review3.78s.

Private loop/device-mapper fixture closes/resumes/unmounts/removes/detaches.
A copied filesystem mounts with loop,ro,noload and yields the expected file;
original andcopied image hashes stay unchanged. Original observed image SHA256
`ae27751cb2238de1807b37fff9d8e805826bd92e79735ef86f4c8235e869cc29`.
Test-temp media is subsequently cleaned up by Go's test harness. This proves
fixture correctness, not originalblockrow fault or retainedmedia qualification.
Complete selectedsource/actualSDK/nativeoutput/review preserved with archive
member and concatenatedSHA256 readback. Nativeprocess is gone.

The first portable block-row launch was rejected by the clean-checkout preflight
while this proof was uncommitted. No evidence root, SDK or server was created.
The preserved preflight log records this ordering issue; the native case did not
run. Launch follows committing the completed proof.
