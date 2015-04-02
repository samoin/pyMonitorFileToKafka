# pyMonitorFileToKafka
python monitor files to kafka

i just have a try.

at the first ,it will find all files ,and search the sync status from zookeeper , get the synced row number ,and read the release file one by one.
at the same time , it will monitor the files change ,such as add ,update , delete.
all of these sync is in one thread ,using some key to make it like a queue , once a time.

i will try to make it with more thread later to improve th speed.

it just suits the increasing log.
this provides another way to monitor files to kafka , or kestrel , or other queen .
if this is well done , i'll try to make it better.
