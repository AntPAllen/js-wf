# Verified duplicate archive reclamation

Removed seven exact duplicate combined temporary proof archives totaling
1,218,708,182 bytes. Their current bytes matched canonical pushed Git parts,
individual part hashes and concatenated archive hashes. Native SDK closure and
all visible task file descriptors were checked immediately before removal. The
lossless delta archive was also verified against its complete canonical base.

Original store fixtures, selected source, executables, canonical Git proof parts,
provider originals, build caches and live campaign stores remain. The 12 GiB
free-space target was not reached; this records actual reclaimed bytes only.
