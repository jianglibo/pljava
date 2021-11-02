## compiel one module

remove example.ddr in the file MANIFEST.MF.
mvn -pl pljava-examples -am clean install

jar tf .\pljava-examples\target\pljava-examples-1.6-SNAPSHOT.jar

## Copy to hostPath

minikube.exe cp .\pljava\pljava-examples\target\pljava-examples-1.6-SNAPSHOT.jar /mnt/data/pljava-examples-1.6-SNAPSHOT.jar

## in the hasura console.

SELECT sqlj.install_jar( 'file:///opt/jars/pljava-examples-1.6-SNAPSHOT.jar', 'dcs_plugin', true );
SELECT sqlj.replace_jar( 'file:///opt/jars/pljava-examples-1.6-SNAPSHOT.jar', 'dcs_plugin', true );