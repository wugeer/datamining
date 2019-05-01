'''
测试一下ods_display
'''

import os
os.system("sqoop --options-file sqoop_option.txt --table ods_display --hive-database rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_display  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1")



