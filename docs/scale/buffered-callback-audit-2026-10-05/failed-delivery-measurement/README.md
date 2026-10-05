# Buffered callback delivery diagnostic failure

Native83d318b fails7.91s. Allsix readers log exact100k sequences/payloads; final
stream metadata retains100k messages but reportsone consumer after successful
delete API calls. Consumer identity/cause was not recorded, so cleanup acceptance
is failed. The test result JSON is absent because the final assertion preceded
its write; independent review parses allsix measurements from the original log.

DirectNext/adapter/Consume/unbufferedcallback/bufferedcallback/Nextrecheck take
342/335/361/361/354/282ms. The ordered/noisy comparison doesnot establish buffered
speedup. Allocation totals include linked in-process R3 server activity.

Independent selectedsource/actualSDK/module/closure review retains the failed
verdict. Original reviewer incorrectly expected PASS and failed; its script/log
are archived. Only the review was corrected, no unchanged native rerun.
Full1088-member /onepart /25,012,213byte archive is read back.

A changed diagnostic will observe names/counts after every deletion within the
original30s reader context, logging metadata convergence separately from delivery.
No buffered correctness/capacity/default/24h adoption or serverdefect claim.
