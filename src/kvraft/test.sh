python dstest.py -p 7 -n 100 -r TestBasic3A
# python dstest.py -p 1 -n 10 -r TestSpeed3A
python dstest.py -p 7 -n 100 -r TestConcurrent3A
python dstest.py -p 7 -n 100 -r TestUnreliable3A
python dstest.py -p 7 -n 100 -r TestUnreliableOneKey3A
python dstest.py -p 7 -n 100 -r TestOnePartition3A
python dstest.py -p 7 -n 100 -r TestManyPartitionsOneClient3A
python dstest.py -p 7 -n 100 -r TestManyPartitionsManyClients3A
python dstest.py -p 7 -n 100 -r TestPersistOneClient3A
python dstest.py -p 7 -n 100 -r TestPersistConcurrent3A
python dstest.py -p 7 -n 100 -r TestPersistConcurrentUnreliable3A
python dstest.py -p 7 -n 100 -r TestPersistPartition3A
python dstest.py -p 7 -n 100 -r TestPersistPartitionUnreliable3A
python dstest.py -p 7 -n 100 -r TestPersistPartitionUnreliableLinearizable3A

python dstest.py -p 7 -n 100 -r TestSnapshotRPC3B
# python dstest.py -p 7 -n 100 -r TestSnapshotSize3B
# python dstest.py -p 7 -n 100 -r TestSpeed3B
python dstest.py -p 7 -n 100 -r TestSnapshotRecover3B
python dstest.py -p 7 -n 100 -r TestSnapshotRecoverManyClients3B
python dstest.py -p 7 -n 100 -r TestSnapshotUnreliable3B
python dstest.py -p 7 -n 100 -r TestSnapshotUnreliableRecover3B
python dstest.py -p 7 -n 100 -r TestSnapshotUnreliableRecoverConcurrentPartition3B
python dstest.py -p 7 -n 100 -r TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B