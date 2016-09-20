test -d output && rm -rf output
mkdir -p output/lib
mkdir -p output/app
mvn -U clean dependency:copy-dependencies -DoutputDirectory=output/lib verify package -Dmaven.test.skip
cd output/app && jar xf ../../target/sloth-0.0.1-SNAPSHOT.jar