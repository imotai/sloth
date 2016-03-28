#!/bin/bash

set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
########################################

WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"


export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}

# boost
if [ -f "boost_1_57_0.tar.gz" ]
then
    echo "boost exist"
else
    echo "start install boost...."
    wget -O boost_1_57_0.tar.gz https://github.com/imotai/common_deps/releases/download/boost/boost_1_57.tar.gz >/dev/null
    tar zxf boost_1_57_0.tar.gz >/dev/null
    rm -rf ${DEPS_PREFIX}/boost_1_57_0
    mv boost_1_57_0 ${DEPS_PREFIX}
    echo "install boost done"
fi

if [ -d "rapidjson" ]
then
    echo "rapid json exist"
else
    echo "start install rapidjson..."
    # rapidjson
    git clone https://github.com/miloyip/rapidjson.git >/dev/null
    rm -rf ${DEPS_PREFIX}/rapidjson
    cp -rf rapidjson ${DEPS_PREFIX}
    echo "install rapidjson done"
fi

if [ -f "Python-2.7.11.tgz" ]
then
    echo "python exist"
else
    echo "start download python"
    wget https://www.python.org/ftp/python/2.7.11/Python-2.7.11.tgz > /dev/null
    tar zxf Python-2.7.11.tgz >/dev/null
    cd Python-2.7.11
    ./configure --prefix=${DEPS_PREFIX}  --disable-shared >/dev/null
    make -j4 && make install>/dev/null
    echo "install python done"
    cd -
fi

if [ -f "setuptools-19.2.tar.gz" ]
then
    echo "setuptools exist"
else
    echo "start download setuptools"
    wget https://pypi.python.org/packages/source/s/setuptools/setuptools-19.2.tar.gz
    tar -zxvf setuptools-19.2.tar.gz >/dev/null
    cd setuptools-19.2
    python setup.py install >/dev/null
    echo "install setuptools done"
    cd -
fi

if [ -f "pip-8.0.2.tar.gz" ]
then
   echo "pip exists"
else
   echo "install pip"
   wget https://pypi.python.org/packages/source/p/pip/pip-8.0.2.tar.gz
   tar -zxvf pip-8.0.2.tar.gz
   cd pip-8.0.2
   python setup.py install >/dev/null
   echo "install pip done"
   cd -
fi


if [ -d "protobuf" ]
then
    echo "protobuf exist"
else
    echo "start install protobuf ..."
    # protobuf
    # wget --no-check-certificate https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz
    git clone --depth=1 https://github.com/00k/protobuf >/dev/null
    mv protobuf/protobuf-2.6.1.tar.gz .
    tar zxf protobuf-2.6.1.tar.gz >/dev/null
    cd protobuf-2.6.1
    ./configure ${DEPS_CONFIG} >/dev/null
    make -j4 >/dev/null
    make install
    cd -
    #cd protobuf-2.6.1/python
    #python setup.py build && python setup.py install
    #cd -
    echo "install protobuf done"
fi

if [ -d "snappy" ]
then
    echo "snappy exist"
else
    echo "start install snappy ..."
    # snappy
    # wget --no-check-certificate https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz
    git clone --depth=1 https://github.com/00k/snappy
    mv snappy/snappy-1.1.1.tar.gz .
    tar zxf snappy-1.1.1.tar.gz >/dev/null
    cd snappy-1.1.1
    ./configure ${DEPS_CONFIG} >/dev/null
    make -j4 >/dev/null
    make install
    cd -
    echo "install snappy done"
fi

if [ -d "sofa-pbrpc_python" ]
then
    echo "sofa exist"
else
    # sofa-pbrpc
    git clone https://github.com/baidu/sofa-pbrpc.git sofa-pbrpc_python
    cd sofa-pbrpc_python
    sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
    sed -i '/PROTOBUF_DIR=/ d' depends.mk
    sed -i '/SNAPPY_DIR=/ d' depends.mk
    echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk
    echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
    cd - 
    cd sofa-pbrpc_python/src
    PROTOBUF_DIR=${DEPS_PREFIX} sh compile_proto.sh ${DEPS_PREFIX}/include
    cd - 
    #cd sofa-pbrpc_python
   # make -j4 >/dev/null
    #make install
    #cd -
    cd sofa-pbrpc_python/python
    python setup.py install
    cd -
fi

if [ -f "sofa-pbrpc-1.0.0.tar.gz" ]
then
    echo "sofa exist"
else
    # sofa-pbrpc
    wget --no-check-certificate -O sofa-pbrpc-1.0.0.tar.gz https://github.com/BaiduPS/sofa-pbrpc/archive/v1.0.0.tar.gz
    tar zxf sofa-pbrpc-1.0.0.tar.gz
    cd sofa-pbrpc-1.0.0
    sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
    sed -i '/PROTOBUF_DIR=/ d' depends.mk
    sed -i '/SNAPPY_DIR=/ d' depends.mk
    echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk
    echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
    cd -
    cd sofa-pbrpc-1.0.0/src
    PROTOBUF_DIR=${DEPS_PREFIX} sh compile_proto.sh
    cd -
    cd sofa-pbrpc-1.0.0
    make -j4 >/dev/null
    make install
    cd -
fi

if [ -f "CMake-3.2.1.tar.gz" ]
then
    echo "CMake-3.2.1.tar.gz exist"
else
    # cmake for gflags
    wget --no-check-certificate -O CMake-3.2.1.tar.gz https://github.com/Kitware/CMake/archive/v3.2.1.tar.gz
    tar zxf CMake-3.2.1.tar.gz
    cd CMake-3.2.1
    ./configure --prefix=${DEPS_PREFIX} >/dev/null
    make -j4 >/dev/null
    make install
    cd -
fi

if [ -f "gflags-2.1.1.tar.gz" ]
then
    echo "gflags-2.1.1.tar.gz exist"
else
    # gflags
    wget --no-check-certificate -O gflags-2.1.1.tar.gz https://github.com/schuhschuh/gflags/archive/v2.1.1.tar.gz
    tar zxf gflags-2.1.1.tar.gz
    cd gflags-2.1.1
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC >/dev/null
    make -j4 >/dev/null
    make install
    cd -
fi

if [ -f "glog-0.3.3.tar.gz" ] 
then
    echo "glog-0.3.3.tar.gz exist"
else
    # glog
    wget --no-check-certificate -O glog-0.3.3.tar.gz https://github.com/google/glog/archive/v0.3.3.tar.gz
    tar zxf glog-0.3.3.tar.gz
    cd glog-0.3.3
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib >/dev/null
    make -j4 >/dev/null
    make install
    cd -
fi

if [ -d "ins" ]
then
    echo "ins exist"
else
    # ins
    git clone https://github.com/fxsjy/ins
    cd ins
    sed -i 's/^SNAPPY_PATH=.*/SNAPPY_PATH=..\/..\/thirdparty/' Makefile
    sed -i 's/^PROTOBUF_PATH=.*/PROTOBUF_PATH=..\/..\/thirdparty/' Makefile
    sed -i 's/^PROTOC_PATH=.*/PROTOC_PATH=..\/..\/thirdparty\/bin/' Makefile
    sed -i 's/^PROTOC=.*/PROTOC=..\/..\/thirdparty\/bin\/protoc/' Makefile
    sed -i 's/^GFLAGS_PATH=.*/GFLAGS_PATH=..\/..\/thirdparty/' Makefile
    sed -i 's/^GTEST_PATH=.*/GTEST_PATH=..\/..\/thirdparty/' Makefile
    sed -i 's/^PREFIX=.*/PREFIX=..\/..\/thirdparty/' Makefile
    export PATH=${DEPS_PREFIX}/bin:$PATH
    export BOOST_PATH=${DEPS_PREFIX}/boost_1_57_0
    export PBRPC_PATH=${DEPS_PREFIX}/
    make -j4 ins && make -j4 install_sdk >/dev/null  && make python >/dev/null
    mkdir -p output/bin && cp ins output/bin
    cd -
fi

if [ -d "googletest" ]
then 
   echo "google test exist"
else
   git clone https://github.com/google/googletest.git
   cd googletest
   cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX}  >/dev/null
   make -j8 >/dev/null && make install
   cd -
fi

if [ -f "yaml-cpp-release-0.5.3.tar.gz"]
then
  echo "yaml cpp exists"
else
  wget -O yaml-cpp-release-0.5.3.tar.gz https://github.com/jbeder/yaml-cpp/archive/release-0.5.3.tar.gz
  tar -zxvf yaml-cpp-release-0.5.3.tar.gz >/dev/null
  cd yaml-cpp-release-0.5.3
  cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX}  -DBUILD_SHARED_LIBS=OFF >/dev/null
  make -j8 >/dev/null && make install
  cd -
fi

if [ -d "common" ]
then 
   echo "common exist"
else

  # common
  git clone https://github.com/baidu/common.git
  cd common
  sed -i 's/^INCLUDE_PATH=.*/INCLUDE_PATH=-Iinclude -I..\/..\/thirdparty\/boost_1_57_0/' Makefile
  make -j8 >/dev/null
  cp -rf include/* ${DEPS_PREFIX}/include
  cp -rf libcommon.a ${DEPS_PREFIX}/lib
  cd -
  cd ${WORK_DIR}
fi

########################################
# build dos
########################################
sh build_version.sh
make -j4


