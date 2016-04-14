## sloth
  
[![Travis CI](https://travis-ci.org/imotai/sloth.svg?branch=sloth)](https://travis-ci.org/imotai/sloth)

### build
```
sh quick_build.sh
```

### run
```
cd sandbox && sh quick_test.sh
1559 pts/0    Sl     0:00 ../sloth --flagfile=sloth.flag --node_idx=0
1560 pts/0    Sl     0:00 ../sloth --flagfile=sloth.flag --node_idx=1
1561 pts/0    Sl     0:00 ../sloth --flagfile=sloth.flag --node_idx=2
1562 pts/0    Sl     0:00 ../sloth --flagfile=sloth.flag --node_idx=3
1563 pts/0    Sl     0:00 ../sloth --flagfile=sloth.flag --node_idx=4
sh show_cluster.sh
  id  endpoint        role       leader          term
-------------------------------------------------------
  0   10.0.2.15:8868  kFollower  10.0.2.15:8869  2
  1   10.0.2.15:8869  kLeader    -               2
  2   10.0.2.15:8870  kFollower  10.0.2.15:8869  2
  3   10.0.2.15:8871  kFollower  10.0.2.15:8869  2
  4   10.0.2.15:8872  kFollower  10.0.2.15:8869  2 
```



