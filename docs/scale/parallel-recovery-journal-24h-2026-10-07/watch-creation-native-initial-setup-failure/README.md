# Initial native watch-creation setup failed

Original clean source6cc1dc3, actual race SDK2528356/SHA d358434fadc30a5f2f80d7be1e6b8b0536e0810404cd3a783916a90ea6bc3cd8, original user unitInvocationIDfd53f3201cbe4fa59cacbaa6a6cc54d6/producer2527446 fails30.05s at CreateKeyValue. The setup deadline is30s; the creation watchdog/fault was not reached. No native recovery acceptance follows and no original deadline is changed.

TCP listener readiness was the only admission before the first R3 bucket request. The corrected fixture explicitly observes an elected metadata leader inside the same30s setup budget before creating the bucket. This removes that admission gap; it does not establish the complete cause of the original ignored/timed-out creation request. The next native execution uses a fresh root and original race/count1/2m/twoGoCPU/1GiB and20s recovery stage.

Source/external before/after, actual live SDK identity, failed log, original failed unit and complete3627-member fixture are retained. Complete archive38,376,975B, SHA256 `cb67997edb0a5f2c1a70d9065c6768eaf94e8be2bd7b405441f4c7d3d7009c5e`. No store reopened or failed verdict repaired.
