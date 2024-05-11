<?php
echo '{"cameras": ';
echo shell_exec("yaml2json /opt/go2rtc/go2rtc.yaml | jq '.streams | keys_unsorted'");
echo '}';
?>