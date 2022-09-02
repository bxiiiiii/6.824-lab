#! /bin/zsh

python dstest.py -p 5 -n 50  TestInitialElection2A TestReElection2A TestManyElections2A
python dstest.py -p 5 -n 50  TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B 

python dstest.py -p 5 -n 100  TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C TestReliableChurn2C TestUnreliableChurn2C
python dstest.py -p 5 -n 10  TestFigure8Unreliable2C