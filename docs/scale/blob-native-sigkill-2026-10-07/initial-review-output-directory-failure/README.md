# Offline review output-directory failure

The original offline reviewer completed its source/actualSDK/archive/native/wire/positive-mutation assertions, then failed creating its new output directory because the parent did not exist. The complete script and traceback are retained. The correction adds parent-directory creation only. The original native SDK and subprocess controls are not rerun; the offline review reads the same original native fixture and archive.
