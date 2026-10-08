# Failed graph-worker development model

Complete seed1 trace from the initial fixture using a workflow identity mapped to a different partition from its consumer. It produced a tight empty-fetch loop and eventually failed its 5s context; saving the 942MiB trace dominated the 65.45s elapsed test. A termination attempt occurred after the SDK was already gone; it was not restarted on an observation timeout. The corrected fixture uses the intended partition and passes all six modes. This failed development artifact remains a failure and establishes no native/release acceptance.
